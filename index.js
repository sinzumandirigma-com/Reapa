"use strict";

/*
 * ZUSIN AI - BANAT-ONLY MESSENGER BOT WITH BLOOD RED DASHBOARD
 * -----------------------------------------------------------
 * No AI. No games. No economy. No RPG. No music. No database.
 */

const fs = require("fs");
const path = require("path");
const http = require("http");
const { login } = require("ws3-fca");
const { getTriggerReply, getBanatConversationReply } = require("./triggers");
const { sendBanatReplyWithTyping } = require("./banat-human");
const { classifyBanatTarget, setBanatConversationMode, isBanatConversationModeActive } = require("./banat-targeting");

// File Paths for Persistence
const ADMINS_PATH = path.join(process.cwd(), "admins.json");
const AUDIT_PATH = path.join(process.cwd(), "audit.json");
const APPSTATE_PATH = path.join(process.cwd(), "appstate.json");

// Load Admins
function loadAdmins() {
  try {
    if (fs.existsSync(ADMINS_PATH)) return JSON.parse(fs.readFileSync(ADMINS_PATH, "utf8"));
  } catch (_) {}
  return [];
}

function saveAdmins(admins) {
  try {
    fs.writeFileSync(ADMINS_PATH, JSON.stringify(admins, null, 2), "utf8");
  } catch (_) {}
}

// Audit Logger
function logAudit(action, details) {
  try {
    const logs = fs.existsSync(AUDIT_PATH) ? JSON.parse(fs.readFileSync(AUDIT_PATH, "utf8")) : [];
    logs.unshift({ timestamp: new Date().toISOString(), action, details });
    if (logs.length > 100) logs.pop();
    fs.writeFileSync(AUDIT_PATH, JSON.stringify(logs, null, 2), "utf8");
  } catch (_) {}
}

// Read Session from appstate.json or cookies.json
function readSession() {
  for (const file of ["appstate.json", "cookies.json"]) {
    const filePath = path.join(process.cwd(), file);
    if (fs.existsSync(filePath)) {
      try {
        return JSON.parse(fs.readFileSync(filePath, "utf8"));
      } catch (_) {
        return fs.readFileSync(filePath, "utf8");
      }
    }
  }
  return null;
}

const PORT = Number(process.env.PORT || 10000);
const DEFAULT_ON = false;
const GLOBAL_SEND_LIMIT = 2;
const THREAD_COOLDOWN_MS = 12000;
const RETRY_DELAYS = [1500, 4000, 8000];

const activeThreads = new Set();
const threadQueues = new Map();
const threadLastSent = new Map();
const threadCooldown = new Map();
let globalActive = 0;
let botUserID = "";
let currentApi = null;

// Bot Status Tracking for Dashboard
const botStats = {
  name: "Zusin AI",
  status: "OFFLINE",
  startTime: null,
  messagesReceived: 0,
  repliesAttempted: 0,
  repliesSuccessful: 0,
  repliesFailed: 0,
  commandsUsed: 0,
  reconnectAttempts: 0,
  connectionErrors: 0
};

function sleep(ms) { return new Promise(resolve => setTimeout(resolve, ms)); }

function normalizeSession(value) {
  if (typeof value === "string") {
    const cookie = value.trim();
    if (!cookie) throw new Error("Facebook cookie session is empty.");
    return cookie;
  }

  const entries =
    Array.isArray(value) ? value :
    Array.isArray(value?.appState) ? value.appState :
    Array.isArray(value?.cookies) ? value.cookies :
    null;

  if (!entries) {
    throw new Error("Facebook session must be a cookie string or a JSON array.");
  }

  const parts = entries
    .map(cookie => {
      const key = cookie?.key ?? cookie?.name;
      const val = cookie?.value;
      if (key == null || val == null) return null;
      return String(key).trim() + "=" + String(val);
    })
    .filter(Boolean);

  if (!parts.length) throw new Error("Facebook session contains no valid cookie entries.");
  return parts.join("; ");
}

function enqueue(threadID, job) {
  const key = String(threadID);
  const current = threadQueues.get(key) || Promise.resolve();
  const next = current.catch(() => {}).then(job).finally(() => {
    if (threadQueues.get(key) === next) threadQueues.delete(key);
  });
  threadQueues.set(key, next);
  return next;
}

async function acquireGlobalSlot() {
  while (globalActive >= GLOBAL_SEND_LIMIT) await sleep(150);
  globalActive++;
}

function releaseGlobalSlot() { globalActive = Math.max(0, globalActive - 1); }

function is1545012(error) {
  const text = JSON.stringify(error || "");
  return /1545012|temporarily unavailable|message could not be sent/i.test(text);
}

function trafficSendMessage(api, message, threadID, callback, replyToMessageID = null) {
  const key = String(threadID);
  botStats.repliesAttempted++;
  return enqueue(key, async () => {
    const now = Date.now();
    const cooldownUntil = Number(threadCooldown.get(key) || 0);
    if (cooldownUntil > now) {
      botStats.repliesFailed++;
      callback(new Error(`thread cooldown active for ${cooldownUntil - now}ms`));
      return;
    }

    const sinceLast = now - Number(threadLastSent.get(key) || 0);
    if (sinceLast < THREAD_COOLDOWN_MS) await sleep(THREAD_COOLDOWN_MS - sinceLast);

    await acquireGlobalSlot();
    try {
      let lastError = null;
      for (let attempt = 0; attempt <= RETRY_DELAYS.length; attempt++) {
        try {
          const result = await new Promise((resolve, reject) => {
            let settled = false;
            const done = (err, info) => {
              if (settled) return;
              settled = true;
              err ? reject(err) : resolve(info);
            };
            try {
              let returned;
              if (replyToMessageID) returned = api.sendMessage(message, threadID, done, replyToMessageID);
              else returned = api.sendMessage(message, threadID, done);
              if (returned && typeof returned.then === "function") returned.then(info => done(null, info)).catch(done);
            } catch (e) { reject(e); }
          });
          threadLastSent.set(key, Date.now());
          botStats.repliesSuccessful++;
          callback(null, result);
          return;
        } catch (error) {
          lastError = error;
          if (!is1545012(error) || attempt >= RETRY_DELAYS.length) break;
          threadCooldown.set(key, Date.now() + Math.min(15000, RETRY_DELAYS[attempt]));
          await sleep(RETRY_DELAYS[attempt]);
          threadCooldown.delete(key);
        }
      }
      if (is1545012(lastError)) threadCooldown.set(key, Date.now() + 5 * 60 * 1000);
      botStats.repliesFailed++;
      callback(lastError);
    } finally {
      releaseGlobalSlot();
    }
  });
}

function isBanatCommand(body) {
  return /^!banat(?:\s|$)/i.test(String(body || "").trim());
}

function commandSendMessage(api, message, threadID, replyToMessageID = null) {
  botStats.repliesAttempted++;
  return new Promise((resolve, reject) => {
    let settled = false;
    const done = (err, info) => {
      if (settled) return;
      settled = true;
      if (err) {
        botStats.repliesFailed++;
        reject(err);
      } else {
        botStats.repliesSuccessful++;
        resolve(info);
      }
    };

    try {
      const returned = replyToMessageID
        ? api.sendMessage(message, threadID, done, replyToMessageID)
        : api.sendMessage(message, threadID, done);

      if (returned && typeof returned.then === "function") {
        returned.then(info => done(null, info)).catch(done);
      }
    } catch (error) {
      done(error);
    }
  });
}

function sendCommandReply(api, event, message) {
  const threadID = String(event.threadID);
  commandSendMessage(api, message, threadID, event.messageID || null).catch(() => {});
}

function handleBanatCommand(api, event, body) {
  botStats.commandsUsed++;
  const threadID = String(event.threadID);
  const parts = String(body).trim().split(/\s+/);
  const sub = (parts[1] || "status").toLowerCase();

  if (sub === "on" || sub === "enable" || sub === "start") {
    setBanatConversationMode(threadID, true, event.senderID);
    activeThreads.add(threadID);
    sendCommandReply(api, event, "Zusin AI banat is on. say whatever u want 😭");
    return true;
  }

  if (sub === "off" || sub === "disable" || sub === "stop") {
    setBanatConversationMode(threadID, false);
    activeThreads.delete(threadID);
    sendCommandReply(api, event, "Zusin AI banat off. peace 😭");
    return true;
  }

  if (sub === "toggle") {
    const next = !isBanatConversationModeActive(threadID);
    setBanatConversationMode(threadID, next, event.senderID);
    if (next) activeThreads.add(threadID);
    else activeThreads.delete(threadID);
    sendCommandReply(api, event, next ? "Zusin AI banat is on 😭" : "Zusin AI banat is off");
    return true;
  }

  if (sub === "status") {
    const on = activeThreads.has(threadID) || isBanatConversationModeActive(threadID);
    sendCommandReply(api, event, on ? "Zusin AI banat: ON 🟢" : "Zusin AI banat: OFF 🔴");
    return true;
  }

  if (sub === "help") {
    sendCommandReply(api, event, "!banat on · !banat off · !banat toggle · !banat status");
    return true;
  }

  sendCommandReply(api, event, "unknown banat command. use !banat help");
  return true;
}

async function sendBanat(api, event, text) {
  return sendBanatReplyWithTyping(api, text, String(event.threadID), event.messageID || null, {
    trafficSendMessage,
    incomingText: event.body || ""
  });
}

function onMessage(api, event) {
  if (!event) return;
  if (event.type && event.type !== "message") return;
  if (event.senderID && botUserID && String(event.senderID) === String(botUserID)) return;

  botStats.messagesReceived++;
  const body = String(event.body || "").trim();
  if (!body) return;
  if (isBanatCommand(body)) { handleBanatCommand(api, event, body); return; }

  const threadID = String(event.threadID);
  const active = activeThreads.has(threadID) || isBanatConversationModeActive(threadID);
  const target = classifyBanatTarget({ event, body, botID: botUserID });

  if (active) {
    const reply = getTriggerReply(body, threadID) || getBanatConversationReply(body, threadID);
    if (reply) {
      sendBanat(api, event, reply).catch(() => {});
    }
    return;
  }

  if (target.shouldRespond) {
    const reply = getTriggerReply(body, threadID) || getBanatConversationReply(body, threadID);
    if (reply) sendBanat(api, event, reply).catch(() => {});
  }
}

function startBot(sessionValue) {
  try {
    const cookie = normalizeSession(sessionValue);
    botStats.status = "CONNECTING...";
    
    login(cookie, (error, api) => {
      if (error) {
        botStats.status = "FAILED";
        botStats.connectionErrors++;
        console.error("[ZUSIN-AI] Login failed:", error?.message || error);
        return;
      }
      
      currentApi = api;
      try { botUserID = String(api.getCurrentUserID?.() || ""); } catch (_) {}
      botStats.status = "ONLINE";
      botStats.startTime = Date.now();

      api.listenMqtt((error, event) => {
        if (error) {
          botStats.connectionErrors++;
          console.error("[ZUSIN-AI] Listener error:", error);
          return;
        }
        try {
          if (DEFAULT_ON && event?.threadID && event?.senderID && String(event.senderID) !== botUserID) {
            const key = String(event.threadID);
            if (!activeThreads.has(key)) {
              activeThreads.add(key);
              setBanatConversationMode(key, true, event.senderID);
            }
          }
          onMessage(api, event);
        } catch (e) {
          console.error("[ZUSIN-AI] Message handler error:", e);
        }
      });
      console.log(`[ZUSIN-AI] Online successfully${botUserID ? ` as ${botUserID}` : ""}`);
    });
  } catch (err) {
    botStats.status = "ERROR";
    console.error("[ZUSIN-AI] Start error:", err.message);
  }
}

// Watchdog & Auto-Reconnect
setInterval(() => {
  if (botStats.status === "FAILED" || botStats.status === "ERROR") {
    const rawSession = readSession();
    if (rawSession) {
      botStats.reconnectAttempts++;
      console.log(`[WATCHDOG] Attempting reconnect #${botStats.reconnectAttempts}...`);
      startBot(rawSession);
    }
  }
}, 30000);

// --- BLOOD RED OPEN DASHBOARD HTML ---
const DASHBOARD_HTML = `<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <title>Zusin AI - Blood Red Dashboard</title>
  <style>
    :root {
      --bg-color: #0a0505;
      --card-bg: #140808;
      --border-color: #3d0a0a;
      --primary: #ff1a1a;
      --primary-hover: #e60000;
      --text: #f0f0f0;
      --text-muted: #a0a0a0;
      --success: #00ff66;
      --danger: #ff3333;
    }
    * { box-sizing: border-box; margin: 0; padding: 0; font-family: monospace, sans-serif; }
    body { background-color: var(--bg-color); color: var(--text); padding: 20px; }
    .container { max-width: 1200px; margin: 0 auto; }
    header { display: flex; justify-content: space-between; align-items: center; border-bottom: 2px solid var(--primary); padding-bottom: 15px; margin-bottom: 25px; }
    h1 { color: var(--primary); font-size: 1.8rem; text-shadow: 0 0 10px rgba(255,26,26,0.5); }
    .btn { background: var(--primary); color: #fff; border: none; padding: 8px 16px; cursor: pointer; font-weight: bold; border-radius: 4px; transition: 0.2s; }
    .btn:hover { background: var(--primary-hover); box-shadow: 0 0 10px var(--primary); }
    .btn-danger { background: var(--danger); }
    .grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(300px, 1fr)); gap: 20px; margin-bottom: 20px; }
    .card { background: var(--card-bg); border: 1px solid var(--border-color); border-radius: 6px; padding: 20px; box-shadow: 0 4px 15px rgba(0,0,0,0.7); }
    .card h2 { color: var(--primary); font-size: 1.2rem; margin-bottom: 15px; border-bottom: 1px solid var(--border-color); padding-bottom: 8px; }
    .stat-row { display: flex; justify-content: space-between; margin-bottom: 10px; font-size: 0.95rem; }
    .stat-val { font-weight: bold; color: var(--text); }
    .status-online { color: var(--success); }
    .status-offline { color: var(--danger); }
    input, textarea, select { width: 100%; background: #1f0c0c; border: 1px solid var(--border-color); color: var(--text); padding: 10px; border-radius: 4px; margin-bottom: 10px; }
    textarea { resize: vertical; height: 80px; }
    table { width: 100%; border-collapse: collapse; margin-top: 10px; }
    th, td { border: 1px solid var(--border-color); padding: 8px; text-align: left; font-size: 0.85rem; }
    th { background: #1f0c0c; color: var(--primary); }
    .logs-box { background: #050202; border: 1px solid var(--border-color); padding: 10px; height: 200px; overflow-y: scroll; font-size: 0.8rem; color: #ff8080; white-space: pre-wrap; }
  </style>
</head>
<body>
  <div class="container">
    <header>
      <h1>ZUSIN AI // DASHBOARD</h1>
      <div>
        <span id="sys-status" class="status-offline" style="font-weight:bold;">● OFFLINE</span>
      </div>
    </header>

    <div class="grid">
      <div class="card">
        <h2>Bot Status & Controls</h2>
        <div class="stat-row"><span>Status:</span> <span id="st-status" class="stat-val">-</span></div>
        <div class="stat-row"><span>Uptime:</span> <span id="st-uptime" class="stat-val">-</span></div>
        <div class="stat-row"><span>Active Threads:</span> <span id="st-threads" class="stat-val">0</span></div>
        <div style="display: flex; gap: 10px; margin-top: 15px;">
          <button class="btn" onclick="controlBot('connect')" style="flex:1;">CONNECT</button>
          <button class="btn btn-danger" onclick="controlBot('disconnect')" style="flex:1;">DISCONNECT</button>
          <button class="btn" onclick="controlBot('reconnect')" style="flex:1;">RECONNECT</button>
        </div>
      </div>

      <div class="card">
        <h2>Live Analytics</h2>
        <div class="stat-row"><span>Messages Received:</span> <span id="an-msg" class="stat-val">0</span></div>
        <div class="stat-row"><span>Replies Attempted:</span> <span id="an-att" class="stat-val">0</span></div>
        <div class="stat-row"><span>Successful Replies:</span> <span id="an-suc" class="stat-val">0</span></div>
        <div class="stat-row"><span>Failed Replies:</span> <span id="an-fail" class="stat-val">0</span></div>
        <div class="stat-row"><span>Commands Used:</span> <span id="an-cmd" class="stat-val">0</span></div>
        <div class="stat-row"><span>Connection Errors:</span> <span id="an-err" class="stat-val">0</span></div>
      </div>

      <div class="card">
        <h2>C3C Session Manager</h2>
        <textarea id="session-input" placeholder="Paste appstate JSON or cookie string here..."></textarea>
        <button class="btn" onclick="updateSession()">UPDATE SESSION</button>
      </div>
    </div>

    <div class="grid">
      <div class="card">
        <h2>Admin Manager</h2>
        <input type="text" id="admin-uid" placeholder="Facebook UID">
        <select id="admin-role">
          <option value="owner">Owner</option>
          <option value="admin">Admin</option>
          <option value="moderator">Moderator</option>
        </select>
        <button class="btn" onclick="addAdmin()">ADD / UPDATE ADMIN</button>
        <table id="admin-table">
          <thead><tr><th>UID</th><th>Role</th><th>Action</th></tr></thead>
          <tbody></tbody>
        </table>
      </div>

      <div class="card">
        <h2>Audit Logs & Status</h2>
        <div id="logs-box" class="logs-box">Loading logs...</div>
      </div>
    </div>
  </div>

  <script>
    async function fetchData() {
      try {
        const res = await fetch("/api/stats");
        const data = await res.json();
        
        document.getElementById("st-status").innerText = data.stats.status;
        const sysStatus = document.getElementById("sys-status");
        sysStatus.innerText = "● " + data.stats.status;
        sysStatus.className = data.stats.status === "ONLINE" ? "status-online" : "status-offline";
        
        document.getElementById("st-uptime").innerText = data.stats.uptime ? Math.floor(data.stats.uptime / 1000) + "s" : "-";
        document.getElementById("st-threads").innerText = data.activeThreadsCount;
        
        document.getElementById("an-msg").innerText = data.stats.messagesReceived;
        document.getElementById("an-att").innerText = data.stats.repliesAttempted;
        document.getElementById("an-suc").innerText = data.stats.repliesSuccessful;
        document.getElementById("an-fail").innerText = data.stats.repliesFailed;
        document.getElementById("an-cmd").innerText = data.stats.commandsUsed;
        document.getElementById("an-err").innerText = data.stats.connectionErrors;

        const adminTbody = document.querySelector("#admin-table tbody");
        adminTbody.innerHTML = "";
        data.admins.forEach(a => {
          adminTbody.innerHTML += '<tr><td>' + a.uid + '</td><td>' + a.role + '</td><td><button class="btn btn-danger" style="padding:2px 6px;" onclick="removeAdmin(\\'' + a.uid + '\\')">X</button></td></tr>';
        });

        document.getElementById("logs-box").innerText = data.logs.map(l => '[' + l.timestamp + '] ' + l.action + ': ' + l.details).join("\\n");
      } catch (e) {}
    }

    async function controlBot(action) {
      await fetch("/api/bot/" + action, { method: "POST" });
      fetchData();
    }

    async function updateSession() {
      const s = document.getElementById("session-input").value;
      const res = await fetch("/api/session", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ session: s })
      });
      const data = await res.json();
      alert(data.message || "Done");
    }

    async function addAdmin() {
      const uid = document.getElementById("admin-uid").value;
      const role = document.getElementById("admin-role").value;
      await fetch("/api/admin", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ uid, role })
      });
      document.getElementById("admin-uid").value = "";
      fetchData();
    }

    async function removeAdmin(uid) {
      await fetch("/api/admin", {
        method: "DELETE",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ uid })
      });
      fetchData();
    }

    fetchData();
    setInterval(fetchData, 3000);
  </script>
</body>
</html>`;

// HTTP Server & Open API Routing
const server = http.createServer((req, res) => {
  const parsedUrl = new URL(req.url, `http://${req.headers.host}`);
  const pathname = parsedUrl.pathname;

  if (pathname === "/health") {
    res.writeHead(200, { "content-type": "application/json" });
    res.end(JSON.stringify({ ok: true, status: botStats.status, service: "zusin-ai" }));
    return;
  }

  if (pathname === "/") {
    res.writeHead(200, { "content-type": "text/html" });
    res.end(DASHBOARD_HTML);
    return;
  }

  if (pathname === "/api/stats" && req.method === "GET") {
    const logs = fs.existsSync(AUDIT_PATH) ? JSON.parse(fs.readFileSync(AUDIT_PATH, "utf8")) : [];
    res.writeHead(200, { "content-type": "application/json" });
    res.end(JSON.stringify({
      stats: {
        ...botStats,
        uptime: botStats.startTime ? Date.now() - botStats.startTime : 0
      },
      activeThreadsCount: activeThreads.size,
      admins: loadAdmins(),
      logs: logs.slice(0, 20)
    }));
    return;
  }

  if (pathname === "/api/bot/connect" && req.method === "POST") {
    const rawSession = readSession();
    if (rawSession) {
      startBot(rawSession);
      logAudit("BOT_CONNECT", "Manual connect triggered");
    }
    res.writeHead(200, { "content-type": "application/json" });
    res.end(JSON.stringify({ success: true }));
    return;
  }

  if (pathname === "/api/bot/disconnect" && req.method === "POST") {
    if (currentApi && typeof currentApi.logout === "function") {
      try { currentApi.logout(() => {}); } catch (_) {}
    }
    currentApi = null;
    botStats.status = "OFFLINE";
    logAudit("BOT_DISCONNECT", "Manual disconnect triggered");
    res.writeHead(200, { "content-type": "application/json" });
    res.end(JSON.stringify({ success: true }));
    return;
  }

  if (pathname === "/api/bot/reconnect" && req.method === "POST") {
    if (currentApi && typeof currentApi.logout === "function") {
      try { currentApi.logout(() => {}); } catch (_) {}
    }
    const rawSession = readSession();
    if (rawSession) {
      startBot(rawSession);
      logAudit("BOT_RECONNECT", "Manual reconnect triggered");
    }
    res.writeHead(200, { "content-type": "application/json" });
    res.end(JSON.stringify({ success: true }));
    return;
  }

  if (pathname === "/api/session" && req.method === "POST") {
    let body = "";
    req.on("data", chunk => body += chunk);
    req.on("end", () => {
      try {
        const { session } = JSON.parse(body);
        let parsedData = session;
        try {
          parsedData = JSON.parse(session);
        } catch (_) {}
        fs.writeFileSync(APPSTATE_PATH, JSON.stringify(parsedData, null, 2), "utf8");
        logAudit("SESSION_UPDATE", "Facebook session updated via dashboard");
        res.writeHead(200, { "content-type": "application/json" });
        res.end(JSON.stringify({ success: true, message: "Session saved to appstate.json successfully!" }));
      } catch (e) {
        res.writeHead(400, { "content-type": "application/json" });
        res.end(JSON.stringify({ error: e.message }));
      }
    });
    return;
  }

  if (pathname === "/api/admin") {
    if (req.method === "POST") {
      let body = "";
      req.on("data", chunk => body += chunk);
      req.on("end", () => {
        try {
          const { uid, role } = JSON.parse(body);
          if (!uid) throw new Error("UID required");
          const admins = loadAdmins();
          const idx = admins.findIndex(a => a.uid === uid);
          if (idx >= 0) admins[idx].role = role;
          else admins.push({ uid, role });
          saveAdmins(admins);
          logAudit("ADMIN_UPDATE", `Updated admin UID ${uid}`);
          res.writeHead(200, { "content-type": "application/json" });
          res.end(JSON.stringify({ success: true }));
        } catch (e) {
          res.writeHead(400, { "content-type": "application/json" });
          res.end(JSON.stringify({ error: e.message }));
        }
      });
      return;
    }
    if (req.method === "DELETE") {
      let body = "";
      req.on("data", chunk => body += chunk);
      req.on("end", () => {
        try {
          const { uid } = JSON.parse(body);
          let admins = loadAdmins();
          admins = admins.filter(a => a.uid !== uid);
          saveAdmins(admins);
          logAudit("ADMIN_REMOVE", `Removed admin UID ${uid}`);
          res.writeHead(200, { "content-type": "application/json" });
          res.end(JSON.stringify({ success: true }));
        } catch (e) {
          res.writeHead(400, { "content-type": "application/json" });
          res.end(JSON.stringify({ error: e.message }));
        }
      });
      return;
    }
  }

  res.writeHead(404, { "content-type": "text/html" });
  res.end("404 Not Found");
});

server.listen(PORT, () => {
  console.log(`[ZUSIN-AI] Dashboard & Health server online on port ${PORT}`);
  const initialSession = readSession();
  if (initialSession) {
    startBot(initialSession);
  } else {
    console.log("[ZUSIN-AI] No appstate.json found. Please configure session via Dashboard.");
  }
});

module.exports = { trafficSendMessage, onMessage, handleBanatCommand };

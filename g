// server.js
// Servidor WebSocket completo: login/register (Firebase), JWT, chat, presença, friends,
// matchmaking, pos, e sistema de barreiras com duração/expiração e broadcast.
// Requisitos: npm install ws firebase-admin bcryptjs jsonwebtoken

const http = require("http");
const WebSocket = require("ws");
const admin = require("firebase-admin");
const bcrypt = require("bcryptjs");
const jwt = require("jsonwebtoken");

const SALT_ROUNDS = 12;
const JWT_SECRET = process.env.JWT_SECRET || "dev_secret_change_this";

// Carrega service account (várias formas suportadas)
let serviceAccount;
if (process.env.FIREBASE_SERVICE_ACCOUNT) {
  try { serviceAccount = JSON.parse(process.env.FIREBASE_SERVICE_ACCOUNT); }
  catch (err) { console.error("FIREBASE_SERVICE_ACCOUNT inválida:", err); process.exit(1); }
} else if (process.env.FIREBASE_SERVICE_ACCOUNT_B64) {
  try {
    const decoded = Buffer.from(process.env.FIREBASE_SERVICE_ACCOUNT_B64, "base64").toString("utf8");
    serviceAccount = JSON.parse(decoded);
  } catch (err) { console.error("FIREBASE_SERVICE_ACCOUNT_B64 inválida:", err); process.exit(1); }
} else {
  try { serviceAccount = require("/etc/secrets/firebase-key.json"); }
  catch (err) { console.error("Nenhuma FIREBASE_SERVICE_ACCOUNT e arquivo /etc/secrets/firebase-key.json não encontrado."); process.exit(1); }
}

admin.initializeApp({
  credential: admin.credential.cert(serviceAccount),
  databaseURL: process.env.FIREBASE_DATABASE_URL || "https://servidor-externo-ff13b-default-rtdb.firebaseio.com/"
});

const db = admin.database();
const PORT = process.env.PORT || 3000;

const server = http.createServer((req, res) => {
  res.writeHead(200, { "Content-Type": "text/plain" });
  res.end("Servidor WebSocket ativo.\n");
});

const wss = new WebSocket.Server({ server });

let clients = {};        // connId -> ws
let nextConnId = 1;

// -------------------------
// Matchmaking / servidores (mapa simples)
let serverStates = {
  1: { scene: "res://scenes/Servidor1.tscn", players: 0, max_players: 10, name: "Servidor Alpha" },
  2: { scene: "res://scenes/Servidor2.tscn", players: 0, max_players: 10, name: "Servidor Beta" },
  3: { scene: "res://scenes/Servidor3.tscn", players: 0, max_players: 10, name: "Servidor Gama" }
};

// -------------------------
// Bases (cada base só pode ter 1 jogador)
const BASES = ["BaseA", "BaseB"];
let baseAssignments = {}; // userId (string) -> baseName
let baseOccupied = {};    // baseName -> connId

function assignBase(userId, connId) {
  for (const base of BASES) {
    if (!baseOccupied[base]) {
      baseOccupied[base] = connId;
      baseAssignments[userId] = base;
      console.log(`assignBase: atribuído ${base} para ${userId} (conn ${connId})`);
      return base;
    }
  }
  console.log(`assignBase: nenhuma base livre para ${userId}`);
  return "";
}

function releaseBase(userId) {
  const base = baseAssignments[userId];
  if (base) {
    console.log(`releaseBase: liberando base ${base} de ${userId}`);
    delete baseAssignments[userId];
    delete baseOccupied[base];
  }
}

function set_assigned_base_for_conn(connId, baseName) {
  if (!baseName) return false;
  if (!BASES.includes(baseName)) {
    console.warn("Base sugerida não encontrada:", baseName);
    return false;
  }
  const occupiedBy = baseOccupied[baseName];
  if (occupiedBy && occupiedBy !== connId) {
    console.warn(`set_assigned_base_for_conn: base ${baseName} já ocupada por conn ${occupiedBy}`);
    return false;
  }
  const connPad = String(connId).padStart(4, "0");
  const prevBase = baseAssignments[connPad];
  if (prevBase && prevBase !== baseName) {
    delete baseOccupied[prevBase];
  }
  baseOccupied[baseName] = connId;
  baseAssignments[connPad] = baseName;
  console.log(`set_assigned_base_for_conn: conn ${connId} agora em ${baseName}`);
  return true;
}

// -------------------------
// Estado das barreiras mantido no servidor
const DEFAULT_BARRIER_SCENE = "res://bar/barreira.tscn";
let baseStates = {}; // base_id -> { base_name, owner, owner_name, active, duration, scene, timeoutId }

function clearBarrierState(baseId) {
  const st = baseStates[baseId];
  if (!st) return;
  if (st.timeoutId) {
    clearTimeout(st.timeoutId);
  }
  delete baseStates[baseId];
}

function createBarrierState(baseId, baseName, owner, owner_name, duration, scenePath = DEFAULT_BARRIER_SCENE) {
  clearBarrierState(baseId);

  const timeoutId = setTimeout(() => {
    clearBarrierState(baseId);
    const payload = { type: "barrier_destroyed", base_id: baseId, base_name: baseName };
    console.log("barrier expired -> broadcasting destroy:", payload);
    broadcastAll(payload);
  }, Math.max(0, duration) * 1000);

  baseStates[baseId] = {
    base_name: baseName,
    owner: owner,
    owner_name: owner_name,
    active: true,
    duration: duration,
    scene: scenePath,
    timeoutId: timeoutId
  };
}

// -------------------------
// Helpers Firebase / presença / friends
async function loadUsers() {
  const snapshot = await db.ref("users").once("value");
  return snapshot.val() || {};
}

async function saveUser(id, userData) {
  await db.ref("users/" + id).set(userData);
}

async function saveFriendship(userId, friendId) {
  await db.ref(`friends/${userId}/${friendId}`).set(true);
  await db.ref(`friends/${friendId}/${userId}`).set(true);
}

async function generateUserId() {
  const users = await loadUsers();
  const ids = Object.keys(users).length > 0 ? Object.keys(users).map(id => parseInt(id)) : [];
  const next = ids.length > 0 ? Math.max(...ids) + 1 : 1;
  return String(next).padStart(4, "0");
}

async function setPresence(userId, online) {
  const ts = Math.floor(Date.now() / 1000);
  await db.ref(`presence/${userId}`).set({ online: !!online, last_active: ts });
  return { online: !!online, last_active: ts };
}

async function getPresence(userId) {
  const snap = await db.ref(`presence/${userId}`).once("value");
  return snap.val() || { online: false, last_active: 0 };
}

async function getFriends(userId) {
  const snap = await db.ref(`friends/${userId}`).once("value");
  return snap.val() ? Object.keys(snap.val()) : [];
}

// -------------------------
// JWT helpers
function createJwt(userId) {
  return jwt.sign({ sub: userId }, JWT_SECRET, { expiresIn: "7d" });
}

function verifyJwt(token) {
  try { return jwt.verify(token, JWT_SECRET); } catch (e) { return null; }
}

// -------------------------
// Utilitários
function safeSend(ws, obj) {
  try {
    if (ws && ws.readyState === WebSocket.OPEN) {
      ws.send(JSON.stringify(obj));
    }
  } catch (e) {
    console.error("safeSend error:", e);
  }
}

function broadcastAll(payload) {
  for (const cid in clients) {
    const c = clients[cid];
    if (!c) continue;
    safeSend(c, payload);
  }
}

function broadcastToFriends(friendsList, payload) {
  for (const cid in clients) {
    const c = clients[cid];
    if (!c) continue;
    if (c.userId && friendsList.includes(c.userId)) {
      safeSend(c, payload);
    }
  }
}

// -------------------------
// Heartbeat / ping-pong server-side
const HEARTBEAT_INTERVAL = 10000; // 10s

const heartbeatInterval = setInterval(async () => {
  for (const cid in clients) {
    const client = clients[cid];
    if (!client) continue;

    if (client.isAlive === false) {
      try { client.terminate(); } catch (e) {}
      const userId = client.userId || String(cid).padStart(4, "0");
      delete clients[cid];
      try { releaseBase(userId); } catch (e) {}
      if (userId) {
        try {
          const presence = await setPresence(userId, false);
          const friendsList = await getFriends(userId);
          if (friendsList.length > 0) {
            broadcastToFriends(friendsList, {
              type: "presence",
              id: userId,
              online: false,
              last_active: presence.last_active
            });
          }
        } catch (err) {
          console.error("Erro ao marcar presença offline:", err);
        }
      }
      continue;
    }

    client.isAlive = false;
    try { client.ping(); } catch (e) {}
  }
}, HEARTBEAT_INTERVAL);

// -------------------------
wss.on("connection", (ws) => {
  const connId = nextConnId++;
  clients[connId] = ws;
  ws.connId = connId;

  ws.isAlive = true;
  ws.on("pong", () => { ws.isAlive = true; });

  const connPad = String(connId).padStart(4, "0");
  assignBase(connPad, connId);

  console.log(`connection: conn ${connId} connected`);

  // envia apenas id no welcome (base será fornecida via request_base/my_base)
  safeSend(ws, { type: "welcome", id: connPad });

  // envia server_list para facilitar a UI do cliente
  safeSend(ws, {
    type: "server_list",
    servers: Object.keys(serverStates).map(k => {
      const s = serverStates[k];
      return { id: parseInt(k), name: s.name, players: s.players, max_players: s.max_players, scene: s.scene };
    })
  });

  // envia estado atual das bases (inclui barreiras ativas) para sincronização imediata
  const statesArray = [];
  for (const bid in baseStates) {
    const s = baseStates[bid];
    statesArray.push({
      base_id: Number(bid),
      base_name: s.base_name,
      owner: s.owner,
      owner_name: s.owner_name,
      active: s.active,
      duration: s.duration,
      scene: s.scene
    });
  }
  if (statesArray.length > 0) {
    safeSend(ws, { type: "base_states", states: statesArray });
  }

  // handler de mensagens (com log de debug)
  ws.on("message", async (msg) => {
    console.log(`message from conn ${connId}:`, msg);
    let data;
    try { data = JSON.parse(msg); } catch { return; }

    // --- notify_barrier: cliente solicita criação de barreira (servidor decide e broadcast) ---
    if (data.type === "notify_barrier") {
      try {
        const baseId = (typeof data.base_id !== "undefined" && data.base_id !== null) ? Number(data.base_id) : -1;
        const baseName = String(data.base || (BASES[baseId] || ""));
        const owner = (typeof data.owner !== "undefined" && data.owner !== null) ? String(data.owner) : (ws.userId ? String(ws.userId) : String(ws.connId).padStart(4, "0"));
        const owner_name = (typeof data.owner_name !== "undefined") ? String(data.owner_name) : (ws.userName || "");
        const duration = typeof data.duration !== "undefined" ? parseInt(data.duration) || 0 : 60;
        const scenePath = typeof data.scene === "string" && data.scene.length > 0 ? data.scene : DEFAULT_BARRIER_SCENE;

        createBarrierState(baseId, baseName, owner, owner_name, duration, scenePath);

        const payload = {
          type: "barrier_created",
          base_id: baseId,
          base_name: baseName,
          scene: scenePath,
          owner: owner,
          owner_name: owner_name,
          duration: duration
        };

        console.log("notify_barrier -> broadcasting barrier_created:", payload);
        broadcastAll(payload);
      } catch (e) {
        console.error("Erro no handler notify_barrier:", e);
      }
      return;
    }

    // --- notify_barrier_destroy: cliente solicita remoção antecipada da barreira ---
    if (data.type === "notify_barrier_destroy" || (data.type === "notify_barrier" && data.active === false)) {
      try {
        const baseId = (typeof data.base_id !== "undefined" && data.base_id !== null) ? Number(data.base_id) : -1;
        const baseName = String(data.base || (BASES[baseId] || ""));
        clearBarrierState(baseId);
        const payload = { type: "barrier_destroyed", base_id: baseId, base_name: baseName };
        console.log("notify_barrier_destroy -> broadcasting:", payload);
        broadcastAll(payload);
      } catch (e) {
        console.error("Erro no handler notify_barrier_destroy:", e);
      }
      return;
    }

    // --- Barrier legacy (compatibilidade) ---
    if (data.type === "barrier") {
      try {
        const baseName = String(data.base || "");
        const baseId = (typeof data.base_id !== "undefined" && data.base_id !== null) ? Number(data.base_id) : -1;
        const owner = (typeof data.owner !== "undefined" && data.owner !== null) ? String(data.owner) : (ws.userId ? String(ws.userId) : String(ws.connId).padStart(4, "0"));
        const owner_name = (typeof data.owner_name !== "undefined") ? String(data.owner_name) : (ws.userName || "");
        const active = !!data.active;
        const duration = typeof data.duration !== "undefined" ? parseInt(data.duration) || 0 : 0;

        if (active) {
          createBarrierState(baseId, baseName, owner, owner_name, duration, DEFAULT_BARRIER_SCENE);
          const payload = {
            type: "barrier_created",
            base_id: baseId,
            base_name: baseName,
            scene: DEFAULT_BARRIER_SCENE,
            owner: owner,
            owner_name: owner_name,
            duration: duration
          };
          console.log("barrier -> broadcasting barrier_created (legacy):", payload);
          broadcastAll(payload);
        } else {
          clearBarrierState(baseId);
          const payload = { type: "barrier_destroyed", base_id: baseId, base_name: baseName };
          console.log("barrier -> broadcasting barrier_destroyed (legacy):", payload);
          broadcastAll(payload);
        }
      } catch (e) {
        console.error("Erro no handler barrier:", e);
      }
      return;
    }

    // --- join_server: pedido do cliente para entrar em um servidor específico ---
    if (data.type === "join_server") {
      try {
        const sid_raw = data.id;
        const sid = typeof sid_raw === "string" && sid_raw.match(/^\d+$/) ? parseInt(sid_raw) : parseInt(sid_raw) || -1;
        console.log(`join_server recebido de conn ${connId} -> sid:`, sid);

        const info = serverStates[sid];
        if (!info) {
          safeSend(ws, { type: "join_fail", id: sid, reason: "invalid" });
          return;
        }

        if (info.players >= info.max_players) {
          safeSend(ws, { type: "join_fail", id: sid, reason: "full" });
          return;
        }

        info.players += 1;
        ws.joinedServerId = sid;
        safeSend(ws, { type: "join_ok", id: sid, scene: info.scene, players: info.players });

        for (const cid in clients) {
          const c = clients[cid];
          if (!c) continue;
          safeSend(c, { type: "server_status", id: sid, players: info.players, max_players: info.max_players });
        }

        console.log(`join_ok enviado para conn ${connId} -> sid ${sid} (players=${info.players})`);
      } catch (e) {
        console.error("Erro no handler join_server:", e);
        safeSend(ws, { type: "join_fail", id: data.id || null, reason: "error" });
      }
      return;
    }

    // --- get_my_base / request_base ---
    if (data.type === "get_my_base") {
      const reqId = String(data.id || "");
      const base = baseAssignments[reqId] || "";
      safeSend(ws, { type: "my_base", id: reqId, base: base });
      return;
    }

    if (data.type === "request_base") {
      const reqId = String(data.player_id || data.player || data.id || "");
      const existing = baseAssignments[reqId];
      if (existing) {
        safeSend(ws, { type: "my_base", id: reqId, base: existing });
        return;
      }
      const base = assignBase(reqId, ws.connId);
      safeSend(ws, { type: "my_base", id: reqId, base: base });
      return;
    }

    // --- request_chat_history ---
    if (data.type === "request_chat_history") {
      try {
        const channel = String(data.channel || "global");
        const limit = 40;
        const snap = await db.ref(`chats/${channel}`).orderByChild("ts").limitToLast(limit).once("value");
        const raw = snap.val() || {};
        const msgs = [];
        for (const k of Object.keys(raw)) {
          const entry = raw[k];
          msgs.push({
            id: String(entry.id || ""),
            from: String((entry.from && entry.from.name) || entry.from || "Anon"),
            avatar: String((entry.from && entry.from.avatar) || ""),
            text: String(entry.text || ""),
            time: parseInt(entry.ts || 0)
          });
        }
        msgs.sort((a, b) => (a.time || 0) - (b.time || 0));
        safeSend(ws, { type: "chat_history", channel, messages: msgs });
      } catch (e) {
        console.error("Erro ao recuperar chat_history:", e);
        safeSend(ws, { type: "chat_history", channel: String(data.channel || "global"), messages: [] });
      }
      return;
    }

    // --- Registro ---
    if (data.type === "register") {
      try {
        const plainPassword = (data.password || "").toString();
        const name = (data.name || "").toString();
        const avatar = (data.avatar || "").toString();

        if (!plainPassword || name.trim() === "") {
          safeSend(ws, { type: "register_fail", reason: "missing_fields" });
          return;
        }

        const userId = await generateUserId();
        const passwordHash = bcrypt.hashSync(plainPassword, SALT_ROUNDS);

        const userData = { id: userId, name: name || userId, avatar: avatar || "", passwordHash, created_at: Date.now() };
        await saveUser(userId, userData);

        ws.userId = userId;
        ws.userName = userData.name;
        const presence = await setPresence(userId, true);

        const token = createJwt(userId);

        // Transferir base temporária (se havia sido atribuída ao conn pad) para o novo userId
        try {
          const connPadLocal = String(ws.connId).padStart(4, "0");
          const assignedToConn = baseAssignments[connPadLocal];
          if (assignedToConn) {
            delete baseAssignments[connPadLocal];
            baseOccupied[assignedToConn] = ws.connId;
            baseAssignments[userId] = assignedToConn;
            console.log(`register: transferida base ${assignedToConn} de conn ${connPadLocal} para user ${userId}`);
          }
        } catch (e) {
          console.error("Erro ao transferir base no register:", e);
        }

        safeSend(ws, { type: "registered", id: userId, name: userData.name, avatar: userData.avatar, online: presence.online, last_active: presence.last_active, token });
      } catch (e) {
        console.error("Erro no register:", e);
        safeSend(ws, { type: "register_fail", reason: "error" });
      }
      return;
    }

    // --- Login ---
    if (data.type === "login") {
      try {
        const id = (data.id || "").toString();
        const plainPassword = (data.password || "").toString();

        if (!id || !plainPassword) {
          safeSend(ws, { type: "login_fail" });
          return;
        }

        const users = await loadUsers();
        const user = users[id];
        if (!user || !user.passwordHash) {
          safeSend(ws, { type: "login_fail" });
          return;
        }

        const ok = bcrypt.compareSync(plainPassword, user.passwordHash);
        if (!ok) {
          safeSend(ws, { type: "login_fail" });
          return;
        }

        ws.userId = id;
        ws.userName = user.name || "";
        const presence = await setPresence(id, true);

        const token = createJwt(id);

        // Transferir base temporária (se havia sido atribuída ao connPad) para o userId real
        try {
          const connPadLocal = String(ws.connId).padStart(4, "0");
          const assignedToConn = baseAssignments[connPadLocal];
          if (assignedToConn) {
            delete baseAssignments[connPadLocal];
            baseOccupied[assignedToConn] = ws.connId;
            baseAssignments[id] = assignedToConn;
            console.log(`login: transferida base ${assignedToConn} de conn ${connPadLocal} para user ${id}`);
          } else {
            const existing = baseAssignments[id];
            if (existing) {
              baseOccupied[existing] = ws.connId;
            }
          }
        } catch (e) {
          console.error("Erro ao transferir base no login:", e);
        }

        safeSend(ws, { type: "login_ok", id: user.id, name: user.name, avatar: user.avatar || "", online: presence.online, last_active: presence.last_active, token });

        // reenviar pedidos pendentes
        const snapshot = await db.ref(`friend_requests/${id}`).once("value");
        const requests = snapshot.val() || {};
        if (Object.keys(requests).length > 0) {
          safeSend(ws, { type: "pending_requests", list: Object.keys(requests) });
        }

        // reenviar lista de amigos + infos (inclui presença atual)
        const snapFriends = await db.ref(`friends/${id}`).once("value");
        const friends = snapFriends.val() || {};
        const infos = [];
        for (const fid of Object.keys(friends)) {
          const friend = users[fid];
          if (friend) {
            const pres = await getPresence(fid);
            infos.push({
              id: friend.id,
              name: friend.name,
              avatar: friend.avatar,
              online: pres.online || false,
              last_active: pres.last_active || 0
            });
          }
        }
        safeSend(ws, { type: "friends_list", id: id, friends: Object.keys(friends), infos });

        // notifica os amigos do usuário que ele está online
        const friendsList = await getFriends(id);
        if (friendsList.length > 0) {
          broadcastToFriends(friendsList, {
            type: "presence",
            id: id,
            online: true,
            last_active: presence.last_active
          });
        }
      } catch (e) {
        console.error("Erro no login:", e);
        safeSend(ws, { type: "login_fail" });
      }
      return;
    }

    // --- suggest_base (cliente sugere base) ---
    if (data.type === "suggest_base") {
      const baseName = String(data.base || "");
      if (baseName && BASES.includes(baseName)) {
        const ok = set_assigned_base_for_conn(ws.connId, baseName);
        if (ok) safeSend(ws, { type: "suggest_base_ok", base: baseName });
        else safeSend(ws, { type: "suggest_base_fail", reason: "occupied" });
      } else {
        safeSend(ws, { type: "suggest_base_fail", reason: "invalid_base" });
      }
      return;
    }

    // --- protected_action (exemplo de rota protegida por token) ---
    if (data.type === "protected_action") {
      const token = data.token || "";
      const payload = verifyJwt(token);
      if (!payload) {
        safeSend(ws, { type: "auth_required" });
        return;
      }
      const userId = payload.sub;
      safeSend(ws, { type: "protected_action_ok", id: userId });
      return;
    }

    // --- get_user_info / friend_request / friend_accept / friend_reject / get_friends ---
    if (data.type === "get_user_info") {
      if (!data.id) return;
      const users = await loadUsers();
      const user = users[data.id];
      if (user) {
        const pres = await getPresence(data.id);
        safeSend(ws, {
          type: "user_info",
          id: user.id,
          name: user.name || user.id,
          avatar: user.avatar || "",
          online: pres.online || false,
          last_active: pres.last_active || 0
        });
      } else {
        safeSend(ws, { type: "user_info", id: data.id, name: data.id, avatar: "", online: false, last_active: 0 });
      }
      return;
    }

    if (data.type === "friend_request") {
      if (!data.to || !data.from) return;
      await db.ref(`friend_requests/${data.to}/${data.from}`).set(true);
      for (const cid in clients) {
        const client = clients[cid];
        if (client && client.userId === data.to) {
          safeSend(client, { type: "friend_request", from: data.from });
        }
      }
      return;
    }

    if (data.type === "get_pending_requests") {
      const snapshot = await db.ref(`friend_requests/${data.id}`).once("value");
      const requests = snapshot.val() || {};
      safeSend(ws, { type: "pending_requests", list: Object.keys(requests) });
      return;
    }

    if (data.type === "friend_accept") {
      if (!data.from || !data.friend) return;
      await saveFriendship(data.from, data.friend);
      await db.ref(`friend_requests/${data.from}/${data.friend}`).remove();
      for (const cid in clients) {
        const client = clients[cid];
        if (!client) continue;
        if (client.userId === data.friend || client.userId === data.from) {
          safeSend(client, { type: "friend_accept", from: data.from, friend: data.friend });
        }
      }
      const presA = await getPresence(data.from);
      const presB = await getPresence(data.friend);
      for (const cid in clients) {
        const client = clients[cid];
        if (client && client.userId === data.friend) {
          safeSend(client, { type: "presence", id: data.from, online: presA.online, last_active: presA.last_active });
        }
        if (client && client.userId === data.from) {
          safeSend(client, { type: "presence", id: data.friend, online: presB.online, last_active: presB.last_active });
        }
      }
      return;
    }

    if (data.type === "friend_reject") {
      if (!data.from || !data.friend) return;
      await db.ref(`friend_requests/${data.from}/${data.friend}`).remove();
      for (const cid in clients) {
        const client = clients[cid];
        if (client && client.userId === data.friend) {
          safeSend(client, { type: "friend_reject", friend: data.from });
        }
      }
      return;
    }

    if (data.type === "get_friends") {
      const users = await loadUsers();
      const snapshot = await db.ref(`friends/${data.id}`).once("value");
      const friends = snapshot.val() || {};
      const infos = [];
      for (const fid of Object.keys(friends)) {
        const friend = users[fid];
        if (friend) {
          const pres = await getPresence(fid);
          infos.push({
            id: friend.id,
            name: friend.name,
            avatar: friend.avatar,
            online: pres.online || false,
            last_active: pres.last_active || 0
          });
        }
      }
      safeSend(ws, { type: "friends_list", id: data.id, friends: Object.keys(friends), infos });
      return;
    }

    // --- pos (posição) ---
    if (data.type === "pos") {
      const now = Date.now();
      if (!ws._lastPosTs) ws._lastPosTs = 0;
      if (now - ws._lastPosTs < 50) return;
      ws._lastPosTs = now;

      const senderConnId = ws.connId;
      const paddedConnId = String(senderConnId).padStart(4, "0");
      let outId = String(data.id || paddedConnId);

      const baseForSender = baseAssignments[outId] || baseAssignments[paddedConnId] || "";

      if (data.base && typeof data.base === "string" && BASES.includes(String(data.base))) {
        set_assigned_base_for_conn(senderConnId, String(data.base));
      }

      function parseRotationAsNumber(v) {
        let n = Number(v);
        if (!isFinite(n)) return 0;
        return n;
      }

      const rx_deg = parseRotationAsNumber(data.rx);
      const ry_deg = parseRotationAsNumber(data.ry);
      const rz_deg = parseRotationAsNumber(data.rz);

      const out = {
        type: "pos",
        id: outId,
        x: typeof data.x !== "undefined" ? data.x : (data[0] || 0),
        y: typeof data.y !== "undefined" ? data.y : (data[1] || 0),
        z: typeof data.z !== "undefined" ? data.z : (data[2] || 0),
        rx: rx_deg,
        ry: ry_deg,
        rz: rz_deg,
        anim: data.anim || data.animation || "idle",
        base: baseForSender,
        name: data.name || "",
        avatar: data.avatar || ""
      };

      if (ws.userId) {
        out.id = String(ws.userId);
        out.name = out.name || ws.userName || out.name;
      } else {
        out.id = out.id || paddedConnId;
      }

      for (const cid in clients) {
        if (parseInt(cid) !== senderConnId) safeSend(clients[cid], out);
      }
      return;
    }

    // --- Chat handler (broadcast para todos) ---
    const CHAT_RATE_LIMIT_MS = 500;
    if (data.type === "chat") {
      try {
        const textRaw = String(data.text ?? data.message ?? data.msg ?? data.texto ?? "");
        const channel = String(data.channel ?? "global");
        const text = textRaw.replace(/\r?\n/g, " ").trim().slice(0, 1000);

        if (text.length === 0) {
          safeSend(ws, { type: "chat_fail", reason: "empty" });
          return;
        }

        const now = Date.now();
        if (!clients[connId]) clients[connId] = ws;
        if (!clients[connId].lastChatTs) clients[connId].lastChatTs = 0;

        if (now - clients[connId].lastChatTs < CHAT_RATE_LIMIT_MS) {
          safeSend(ws, { type: "chat_fail", reason: "rate_limited" });
          return;
        }
        clients[connId].lastChatTs = now;

        let publicFrom = { id: "", name: "Anon", avatar: "" };
        if (ws.userId) {
          try {
            const users = await loadUsers();
            const u = users[ws.userId];
            if (u) {
              publicFrom = { id: String(u.id || ""), name: String(u.name || "Anon"), avatar: String(u.avatar || "") };
            } else if (ws.userName) {
              publicFrom = { id: String(ws.userId), name: String(ws.userName || "Anon"), avatar: "" };
            }
          } catch (e) {
            console.error("Erro ao carregar usuário para chat:", e);
          }
        }

        if ((publicFrom.name === "Anon" || publicFrom.name === "") && data.name) {
          publicFrom.name = String(data.name).slice(0, 64);
        }

        const senderIdForClients = ws.userId ? String(ws.userId) : String(ws.connId).padStart(4, "0");

        const payload = {
          type: "chat",
          id: senderIdForClients,
          from: publicFrom.name,
          name: publicFrom.name,
          avatar: publicFrom.avatar,
          text: text,
          channel: channel,
          ts: now
        };

        for (const cid in clients) {
          const c = clients[cid];
          if (!c) continue;
          safeSend(c, payload);
        }

        try {
          const ref = db.ref(`chats/${channel}`);
          await ref.push({ id: senderIdForClients, from: publicFrom, text, ts: now });

          const snap = await ref.orderByChild("ts").once("value");
          const all = snap.val() || {};
          const keys = Object.keys(all);
          if (keys.length > 40) {
            const sorted = keys.sort((a, b) => (all[a].ts || 0) - (all[b].ts || 0));
            const toRemove = sorted.slice(0, keys.length - 40);
            for (const k of toRemove) {
              try { await ref.child(k).remove(); } catch (e) {}
            }
          }
        } catch (e) {
          console.error("Erro salvar chat:", e);
        }

      } catch (err) {
        console.error("Erro no handler de chat:", err, "raw:", msg);
      }
      return;
    }

    // --- Ping/Pong via JSON (opcional) ---
    if (data.type === "ping") {
      safeSend(ws, { type: "pong", time: data.time });
      return;
    }

    // --- get_presence / set_presence ---
    if (data.type === "get_presence") {
      if (!data.id) return;
      const pres = await getPresence(data.id);
      safeSend(ws, { type: "get_presence_response", id: data.id, online: pres.online || false, last_active: pres.last_active || 0 });
      return;
    }

    if (data.type === "set_presence") {
      if (!data.id) return;
      const presence = await setPresence(data.id, !!data.online);
      const friendsList = await getFriends(data.id);
      if (friendsList.length > 0) {
        broadcastToFriends(friendsList, {
          type: "presence",
          id: data.id,
          online: presence.online,
          last_active: presence.last_active
        });
      }
      return;
    }

  }); // fim ws.on("message")

  ws.on("close", async () => {
    const userId = ws.userId || String(connId).padStart(4, "0");
    console.log(`close: conn ${connId} closed, userId ${userId}`);

    try {
      if (ws.joinedServerId) {
        const sid = ws.joinedServerId;
        if (serverStates[sid]) {
          serverStates[sid].players = Math.max(0, serverStates[sid].players - 1);
          for (const cid in clients) {
            const c = clients[cid];
            if (!c) continue;
            safeSend(c, { type: "server_status", id: sid, players: serverStates[sid].players, max_players: serverStates[sid].max_players });
          }
          console.log(`conn ${connId} saiu do server ${sid}, players agora ${serverStates[sid].players}`);
        }
      }
    } catch (e) {
      console.error("Erro ao decrementar serverState no close:", e);
    }

    delete clients[connId];

    try { releaseBase(userId); } catch (e) { console.error("Erro ao liberar base no close:", e); }

    try {
      const presence = await setPresence(userId, false);
      const friendsList = await getFriends(userId);
      if (friendsList.length > 0) {
        broadcastToFriends(friendsList, {
          type: "presence",
          id: userId,
          online: false,
          last_active: presence.last_active
        });
      }
    } catch (e) {
      console.error("Erro ao setPresence no close:", e);
    }

  });

  ws.on("error", (err) => {
    console.error(`ws error conn ${connId}:`, err);
  });

}); // fim wss.on connection

// Graceful shutdown
function shutdown() {
  console.log("Shutting down server...");
  clearInterval(heartbeatInterval);
  try {
    wss.clients.forEach((c) => {
      try { c.close(); } catch (e) {}
    });
    server.close(() => {
      console.log("HTTP server closed.");
      process.exit(0);
    });
  } catch (e) {
    console.error("Erro no shutdown:", e);
    process.exit(1);
  }
}

process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);

// Start server
server.listen(PORT, () => {
  console.log(`WebSocket server listening on port ${PORT}`);
});
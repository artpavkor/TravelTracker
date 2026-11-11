/**
 * TravelTracker — сервер реального времени (Node.js + Express + Socket.io)
 *
 * Логика:
 *  - Статическая отдача index.html (и всего, что лежит рядом)
 *  - Комнаты поездок: клиент подключается с ?room=<название> и присылает событие join
 *  - Каждая комната держит свое состояние: участники с координатами, батареей, статусом
 *  - Сервер раз в INACTIVE_TIMEOUT мс выкидывает "зависших" (не слали координаты)
 *  - Рассылка обновлений — только внутри комнаты
 */

const path = require('path');
const http = require('http');
const express = require('express');
const { Server } = require('socket.io');

const PORT = process.env.PORT || 3000;

// Сколько мс участник может не слать координаты, прежде чем считаться оффлайн
const INACTIVE_TIMEOUT = 60_000;
// Как часто сервер проверяет активность
const SWEEP_INTERVAL = 15_000;

const app = express();
app.use(express.json());
// Отдаём статику (index.html и пр.)
app.use(express.static(__dirname, { maxAge: '1h' }));

const server = http.createServer(app);
const io = new Server(server, {
  cors: { origin: '*' }, // в проде сузьте origin до домена Mini App
  pingInterval: 10_000,
  pingTimeout: 20_000,
});

/**
 * Структура комнат:
 * rooms = Map<roomId, Map<socketId, participant>>
 * participant = { id, name, avatar, color, lat, lng, battery, status, lastActive }
 */
const rooms = new Map();

const COLORS = ['#FF6B6B', '#4ECDC4', '#45B7D1', '#96CEB4', '#FECA57', '#A29BFE', '#FD79A8', '#00CEC9'];
let colorCursor = 0;
const nextColor = () => COLORS[colorCursor++ % COLORS.length];

function getRoom(roomId) {
  if (!rooms.has(roomId)) rooms.set(roomId, new Map());
  return rooms.get(roomId);
}

/** Сериализуем участников для отправки клиентам */
function roomSnapshot(room) {
  return [...room.values()].map((p) => ({
    id: p.id,
    name: p.name,
    avatar: p.avatar,
    color: p.color,
    lat: p.lat,
    lng: p.lng,
    battery: p.battery,
    status: p.status,
    lastActive: p.lastActive,
  }));
}

io.on('connection', (socket) => {
  let currentRoom = null;
  let me = null;

  // Клиент присоединяется к комнате поездки
  socket.on('join', (payload = {}) => {
    const roomId = String(payload.room || 'default').slice(0, 64);
    const name = String(payload.name || 'Аноним').slice(0, 40);
    const avatar = String(payload.avatar || '').slice(0, 500);

    // Если уже был в другой комнате — выходим
    if (currentRoom) socket.leave(currentRoom);

    currentRoom = roomId;
    socket.join(roomId);

    const room = getRoom(roomId);
    me = {
      id: socket.id,
      name,
      avatar,
      color: nextColor(),
      lat: payload.lat ?? null,
      lng: payload.lng ?? null,
      battery: payload.battery ?? null,
      status: payload.status || '🧭 В пути',
      lastActive: Date.now(),
    };
    room.set(socket.id, me);

    console.log(`[+] ${name} (${socket.id}) вошёл в комнату "${roomId}" (${room.size} онлайн)`);

    // Полный снапшот комнаты — новому участнику
    socket.emit('room:state', { room: roomId, users: roomSnapshot(room) });
    // Остальным — что пришёл новый
    socket.to(roomId).emit('user:joined', me);
  });

  // Обновление геопозиции + батарея
  socket.on('location:update', (payload = {}) => {
    if (!currentRoom || !me) return;
    const lat = Number(payload.lat);
    const lng = Number(payload.lng);
    if (!Number.isFinite(lat) || !Number.isFinite(lng)) return;
    if (Math.abs(lat) > 90 || Math.abs(lng) > 180) return;

    me.lat = lat;
    me.lng = lng;
    if (payload.battery !== undefined) me.battery = Math.max(0, Math.min(100, Number(payload.battery)));
    me.lastActive = Date.now();

    // Ретранслируем всем в комнате, кроме отправителя
    socket.to(currentRoom).emit('user:moved', {
      id: me.id, lat: me.lat, lng: me.lng, battery: me.battery, lastActive: me.lastActive,
    });
  });

  // Смена статуса участника
  socket.on('status:update', (payload = {}) => {
    if (!currentRoom || !me) return;
    me.status = String(payload.status || '').slice(0, 60) || '🧭 В пути';
    socket.to(currentRoom).emit('user:status', { id: me.id, status: me.status });
  });

  socket.on('disconnect', () => {
    if (currentRoom && me) {
      const room = rooms.get(currentRoom);
      if (room) {
        room.delete(socket.id);
        socket.to(currentRoom).emit('user:left', { id: socket.id });
        // Чистим пустые комнаты, чтобы не копить память
        if (room.size === 0) rooms.delete(currentRoom);
      }
      console.log(`[-] ${me.name} покинул "${currentRoom}"`);
    }
  });
});

/** Уборщик: выкидываем неактивных из всех комнат */
setInterval(() => {
  const now = Date.now();
  for (const [roomId, room] of rooms) {
    for (const [socketId, p] of room) {
      if (now - p.lastActive > INACTIVE_TIMEOUT) {
        room.delete(socketId);
        io.to(roomId).emit('user:left', { id: socketId });
        console.log(`[⏱] ${p.name} удалён из "${roomId}" по таймауту неактивности`);
      }
    }
    if (room.size === 0) rooms.delete(roomId);
  }
}, SWEEP_INTERVAL);

// Health-check (удобно для мониторинга/деплоя)
app.get('/health', (_req, res) => {
  res.json({
    ok: true,
    rooms: rooms.size,
    online: [...rooms.values()].reduce((n, r) => n + r.size, 0),
    uptime: Math.round(process.uptime()),
  });
});

server.listen(PORT, () => {
  console.log(`🚀 TravelTracker слушает порт ${PORT}`);
});

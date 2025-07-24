const jwt = require('jsonwebtoken');
const { jwtSecret } = require('../config/keys');
const redisClient = require('../utils/redisClient');
const amqp = require('amqplib');
const Message = require('../models/Message');
const Room = require('../models/Room');
const User = require('../models/User');
const SessionService = require('../services/sessionService');
const aiService = require('../services/aiService').default;

let mqChannel = null;
async function getMQChannel() {
  if (!mqChannel) {
    const conn = await amqp.connect(process.env.RABBITMQ_URL || 'amqp://localhost');
    mqChannel = await conn.createChannel();
    await mqChannel.assertQueue('chat-messages', { durable: true });
  }
  return mqChannel;
}

let ioInstance = null;

function setSocketIO(io) {
  ioInstance = io;

  // 메모리맵(단일노드만!)
  const connectedUsers = new Map();
  const userRooms = new Map();

  // 미들웨어: JWT 인증 + 세션 검증
  io.use(async (socket, next) => {
    try {
      const { token, sessionId } = socket.handshake.auth;
      if (!token || !sessionId) return next(new Error('인증 필요'));
      const decoded = jwt.verify(token, jwtSecret);
      if (!decoded?.user?.id) return next(new Error('토큰 오류'));
      const user = await User.findById(decoded.user.id);
      if (!user) return next(new Error('존재하지 않는 유저'));
      const valid = await SessionService.validateSession(decoded.user.id, sessionId);
      if (!valid.isValid) return next(new Error(valid.message));
      socket.user = { id: user._id.toString(), name: user.name, profileImage: user.profileImage, sessionId };
      await SessionService.updateLastActivity(user._id);
      next();
    } catch (e) {
      return next(new Error('인증 오류'));
    }
  });

  io.on('connection', (socket) => {
    const userId = socket.user.id;
    // 단일 노드: 중복 로그인이면 이전 연결 끊기
    if (connectedUsers.has(userId)) {
      const prevSocketId = connectedUsers.get(userId);
      if (prevSocketId !== socket.id && io.sockets.sockets.get(prevSocketId)) {
        io.sockets.sockets.get(prevSocketId).emit('session_ended', { reason: 'duplicate_login' });
        io.sockets.sockets.get(prevSocketId).disconnect(true);
      }
    }
    connectedUsers.set(userId, socket.id);

    // 채팅방 입장
    socket.on('joinRoom', async (roomId) => {
      const room = await Room.findById(roomId).populate('participants', 'name profileImage');
      console.log('participantsUpdate:', room && room.participants);
      if (!room || !room.participants.some(p => String(p._id) === userId)) {
        return socket.emit('joinRoomError', { message: '입장 권한 없음' });
      }
      socket.join(roomId);
      userRooms.set(userId, roomId);

      // 입장 메시지 저장/전파
      const joinMsg = await Message.create({
        room: roomId,
        content: `${socket.user.name}님이 입장하였습니다.`,
        type: 'system',
        timestamp: new Date()
      });
      await cacheMessageToRedis(roomId, joinMsg);
      io.to(roomId).emit('message', joinMsg);
      io.to(roomId).emit('participantsUpdate', room.participants);

      // 초기 메시지 전송
      const messages = await getMessagesFromCacheOrDb(roomId, null, 30);
      socket.emit('joinRoomSuccess', {
        roomId, participants: room.participants, messages: messages.items, hasMore: messages.hasMore
      });
    });

    // 메시지 전송
    socket.on('chatMessage', async (msg) => {
      const { room, content } = msg;
      // 권한, 세션 검증
      const roomObj = await Room.findById(room);
      if (!roomObj || !roomObj.participants.includes(userId)) return;
      // AI 멘션 파싱
      const aiMentions = extractAIMentions(content);
      const sendMsg = {
        ...msg, sender: userId, timestamp: new Date(), aiMentions
      };
      // 메시지 큐로 push (실제 소비자는 별도 worker에서 처리해도 됨)
      const channel = await getMQChannel();
      await channel.sendToQueue('chat-messages', Buffer.from(JSON.stringify(sendMsg)), { persistent: true });

      // AI 작업 큐
      for (const ai of aiMentions) {
        const query = content.replace(new RegExp(`@${ai}\\b`, 'g'), '').trim();
        await aiService.publishAITask({ room, aiName: ai, query });
      }
    });

    // 퇴장
    socket.on('leaveRoom', async (roomId) => {
      socket.leave(roomId);
      userRooms.delete(userId);

      // 퇴장 메시지
      const leaveMsg = await Message.create({
        room: roomId,
        content: `${socket.user.name}님이 퇴장하였습니다.`,
        type: 'system',
        timestamp: new Date()
      });
      await cacheMessageToRedis(roomId, leaveMsg);

      // 참가자 DB/캐시 최신화
      await Room.findByIdAndUpdate(roomId, { $pull: { participants: userId } });
      const room = await Room.findById(roomId).populate('participants', 'name profileImage');
      io.to(roomId).emit('message', leaveMsg);
      io.to(roomId).emit('participantsUpdate', room.participants);
    });

    // 이전 메시지 로딩
    socket.on('fetchPreviousMessages', async ({ roomId, before }) => {
      const result = await getMessagesFromCacheOrDb(roomId, before, 30);
      socket.emit('previousMessagesLoaded', {
        messages: result.items, hasMore: result.hasMore
      });
    });

    // 연결 해제
    socket.on('disconnect', async () => {
      connectedUsers.delete(userId);
      const roomId = userRooms.get(userId);
      if (!roomId) return;
      userRooms.delete(userId);

      // 시스템 메시지로 퇴장 broadcast
      const leaveMsg = await Message.create({
        room: roomId,
        content: `${socket.user.name}님이 연결이 끊어졌습니다.`,
        type: 'system',
        timestamp: new Date()
      });
      await cacheMessageToRedis(roomId, leaveMsg);
      await Room.findByIdAndUpdate(roomId, { $pull: { participants: userId } });
      const room = await Room.findById(roomId).populate('participants', 'name profileImage');
      io.to(roomId).emit('message', leaveMsg);
      io.to(roomId).emit('participantsUpdate', room.participants);
    });
  });

  // Redis 메시지 캐싱 util
  async function cacheMessageToRedis(roomId, messageObj) {
    const redisKey = `chat:room:${roomId}:messages`;
    await redisClient.lPush(redisKey, JSON.stringify(messageObj));
    await redisClient.lTrim(redisKey, 0, 99);
    await redisClient.expire(redisKey, 86400); // 1일 TTL
  }

  // 메시지 캐시 우선 로딩 util
  async function getMessagesFromCacheOrDb(roomId, before, limit) {
    const redisKey = `chat:room:${roomId}:messages`;
    let cached = await redisClient.lRange(redisKey, 0, limit + 1);
    let messages = cached.map(x => { try { return JSON.parse(x); } catch { return null; } }).filter(Boolean);
    if (before) messages = messages.filter(msg => new Date(msg.timestamp) < new Date(before));
    messages = messages.sort((a, b) => new Date(b.timestamp) - new Date(a.timestamp));
    const hasMore = messages.length > limit;
    const items = messages.slice(0, limit).sort((a, b) => new Date(a.timestamp) - new Date(b.timestamp));
    if (items.length > 0) return { items, hasMore };
    // 캐시 miss → DB
    let dbMessages = await Message.find({ room: roomId })
      .sort({ timestamp: -1 }).limit(limit + 1).lean();
    if (before) dbMessages = dbMessages.filter(msg => new Date(msg.timestamp) < new Date(before));
    dbMessages = dbMessages.sort((a, b) => new Date(b.timestamp) - new Date(a.timestamp));
    const hasMoreDb = dbMessages.length > limit;
    const itemsDb = dbMessages.slice(0, limit).sort((a, b) => new Date(a.timestamp) - new Date(b.timestamp));
    if (itemsDb.length > 0) await redisClient.lPush(redisKey, ...itemsDb.map(m => JSON.stringify(m)));
    return { items: itemsDb, hasMore: hasMoreDb };
  }

  // AI 멘션 추출
  function extractAIMentions(content) {
    if (!content) return [];
    return (content.match(/@(wayneAI|consultingAI)\b/g) || []).map(x => x.replace('@',''));
  }

  return io;
}

function getSocketIO() {
  if (!ioInstance) throw new Error('Socket.IO 인스턴스가 설정되지 않았습니다.');
  return ioInstance;
}

module.exports = { setSocketIO, getSocketIO };

const jwt = require('jsonwebtoken');
const { jwtSecret } = require('../config/keys');
const redis = require('../utils/redisClient');
const amqp = require('amqplib');
const User = require('../models/User');
const SessionService = require('../services/sessionService');
const aiService = require('../services/aiService').default;
const { nanoid } = require('nanoid');

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

  // 메모리맵 (싱글노드)
  const connectedUsers = new Map();
  const userRooms = new Map();
  const BATCH_SIZE = 30;

  // 소켓 인증 미들웨어 (JWT+세션)
  io.use(async (socket, next) => {
    try {
      const { token, sessionId } = socket.handshake.auth;
      if (!token || !sessionId) return next(new Error('인증 필요'));
      const decoded = jwt.verify(token, jwtSecret);
      if (!decoded?.user?.id) return next(new Error('토큰 오류'));

      // 유저 정보는 Redis에서, 없으면 MongoDB fallback
      let user = await redis.hGetAll(`user:${decoded.user.id}`);
      if (!user || !user.id) {
        const dbUser = await User.findById(decoded.user.id);
        if (!dbUser) return next(new Error('존재하지 않는 유저'));
        user = {
          id: dbUser._id.toString(),
          name: dbUser.name,
          email: dbUser.email,
          profileImage: dbUser.profileImage || ''
        };
        await redis.hSet(`user:${dbUser._id}`, user);
      }
      // 세션 유효성
      const valid = await SessionService.validateSession(user.id, sessionId);
      if (!valid.isValid) return next(new Error(valid.message));
      socket.user = { ...user, sessionId };
      await SessionService.updateLastActivity(user.id);
      next();
    } catch (e) {
      return next(new Error('인증 오류'));
    }
  });

  io.on('connection', (socket) => {
    const userId = socket.user.id;

    // 중복 로그인 처리(이전 소켓 강제 종료)
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
      try {
        // 이전 방 나가기
        const prevRoom = userRooms.get(userId);
        if (prevRoom && prevRoom !== roomId) {
          socket.leave(prevRoom);
          userRooms.delete(userId);
          let prevRoomData = await redis.hGetAll(`room:${prevRoom}`);
          if (prevRoomData && prevRoomData.participants) {
            let prevList = JSON.parse(prevRoomData.participants);
            prevList = prevList.filter(id => id !== userId);
            await redis.hSet(`room:${prevRoom}`, { participants: JSON.stringify(prevList) });
            io.to(prevRoom).emit('participantsUpdate', await getParticipantArr(prevRoom));
          }
        }
        // 참가자 추가
        let room = await redis.hGetAll(`room:${roomId}`);
        if (!room) return socket.emit('joinRoomError', { message: '채팅방 없음' });
        let participants = room.participants ? JSON.parse(room.participants) : [];
        if (!participants.includes(userId)) {
          participants.push(userId);
          await redis.hSet(`room:${roomId}`, { participants: JSON.stringify(participants) });
        }
        socket.join(roomId);
        userRooms.set(userId, roomId);
        // 입장 메시지 저장
        const joinMsg = {
          _id: nanoid(),
          room: roomId,
          content: `${socket.user.name}님이 입장하였습니다.`,
          type: 'system',
          sender: null,
          timestamp: Date.now()
        };
        await redis.rPush(`chat:messages:${roomId}`, JSON.stringify(joinMsg));
        // 참가자 목록 최신화
        const participantArr = await getParticipantArr(roomId);
        // 메시지 페이징
        const total = await redis.lLen(`chat:messages:${roomId}`);
        const start = Math.max(0, total - BATCH_SIZE);
        const end = total - 1;
        const msgStrs = await redis.lRange(`chat:messages:${roomId}`, start, end);
        const messages = msgStrs.map(s => { try { return JSON.parse(s); } catch { return null; } }).filter(Boolean);
        const hasMore = start > 0;
        const oldestTimestamp = messages.length > 0 ? messages[0].timestamp : null;
        // 입장 메시지, 참가자 emit
        io.to(roomId).emit('message', joinMsg);
        io.to(roomId).emit('participantsUpdate', participantArr);
        // 본인에게 전체 메시지 전달
        socket.emit('joinRoomSuccess', {
          roomId, participants: participantArr, messages, hasMore, oldestTimestamp
        });
      } catch (error) {
        socket.emit('joinRoomError', { message: error.message });
      }
    });

    // 메시지 전송
    socket.on('chatMessage', async (msg) => {
      try {
        const { room, content, type, fileData } = msg;
        // 방 권한 체크
        const roomData = await redis.hGetAll(`room:${room}`);
        if (!roomData) return;
        let participants = roomData.participants ? JSON.parse(roomData.participants) : [];
        if (!participants.includes(userId)) return;

        // AI 멘션 추출
        const aiMentions = extractAIMentions(content);

        // 메시지 오브젝트 생성
        let messageObj;
        if (type === 'file') {
          let fileMeta = fileData || {};
          messageObj = {
            room,
            sender: { id: userId, name: socket.user.name, email: socket.user.email, profileImage: socket.user.profileImage },
            type: 'file',
            file: fileMeta,
            content: content || '',
            timestamp: Date.now(),
            reactions: {},
            metadata: fileMeta
          };
        } else {
          messageObj = {
            room,
            sender: { id: userId, name: socket.user.name, email: socket.user.email, profileImage: socket.user.profileImage },
            content: (content || '').trim(),
            type: 'text',
            timestamp: Date.now(),
            reactions: {}
          };
        }
        await redis.rPush(`chat:messages:${room}`, JSON.stringify(messageObj));
        io.to(room).emit('message', messageObj);

        // 메시지 큐에도 발행(비동기)
        const channel = await getMQChannel();
        channel.sendToQueue('chat-messages', Buffer.from(JSON.stringify({ ...msg, sender: userId, timestamp: new Date(), aiMentions })), { persistent: true });

        // AI 멘션 있으면 처리
        for (const ai of aiMentions) {
          const query = content.replace(new RegExp(`@${ai}\\b`, 'g'), '').trim();
          await aiService.publishAITask({ room, aiName: ai, query });
        }
      } catch (error) {
        socket.emit('error', { code: error.code || 'MESSAGE_ERROR', message: error.message });
      }
    });

    // 퇴장
    socket.on('leaveRoom', async (roomId) => {
      try {
        socket.leave(roomId);
        userRooms.delete(userId);
        let room = await redis.hGetAll(`room:${roomId}`);
        let participants = room.participants ? JSON.parse(room.participants) : [];
        participants = participants.filter(id => id !== userId);
        await redis.hSet(`room:${roomId}`, { participants: JSON.stringify(participants) });
        const leaveMsg = {
          _id: nanoid(),
          room: roomId,
          content: `${socket.user.name}님이 퇴장하였습니다.`,
          type: 'system',
          sender: null,
          timestamp: Date.now()
        };
        await redis.rPush(`chat:messages:${roomId}`, JSON.stringify(leaveMsg));
        const participantArr = await getParticipantArr(roomId);
        io.to(roomId).emit('message', leaveMsg);
        io.to(roomId).emit('participantsUpdate', participantArr);
      } catch (error) {
        socket.emit('error', { message: error.message });
      }
    });

    // 이전 메시지 로딩
    socket.on('fetchPreviousMessages', async ({ roomId, before }) => {
      try {
        const total = await redis.lLen(`chat:messages:${roomId}`);
        let start, end;
        if (!before) {
          end = total - 1;
          start = Math.max(0, end - BATCH_SIZE + 1);
        } else {
          const allMsgs = await redis.lRange(`chat:messages:${roomId}`, 0, total - 1);
          let idx = allMsgs.findIndex(msgStr => {
            try { const msg = JSON.parse(msgStr); return msg && msg.timestamp === before; } catch { return false; }
          });
          if (idx === -1) idx = allMsgs.length;
          end = idx - 1;
          start = Math.max(0, end - BATCH_SIZE + 1);
        }
        const messageStrings = await redis.lRange(`chat:messages:${roomId}`, start, end);
        const messages = messageStrings.map(s => { try { return JSON.parse(s); } catch { return null; } }).filter(Boolean);
        messages.sort((a, b) => a.timestamp - b.timestamp);
        const oldestTimestamp = messages.length > 0 ? messages[0].timestamp : null;
        const hasMore = start > 0;
        socket.emit('previousMessagesLoaded', { messages, hasMore, oldestTimestamp });
      } catch (error) {
        socket.emit('error', { type: 'LOAD_ERROR', message: error.message });
      }
    });

    // 연결 해제
    socket.on('disconnect', async () => {
      connectedUsers.delete(userId);
      const roomId = userRooms.get(userId);
      userRooms.delete(userId);
      if (roomId) {
        let room = await redis.hGetAll(`room:${roomId}`);
        if (room) {
          let participants = room.participants ? JSON.parse(room.participants) : [];
          participants = participants.filter(id => id !== userId);
          await redis.hSet(`room:${roomId}`, { participants: JSON.stringify(participants) });
          const leaveMsg = {
            _id: nanoid(),
            room: roomId,
            content: `${socket.user.name}님이 연결이 끊어졌습니다.`,
            type: 'system',
            sender: null,
            timestamp: Date.now()
          };
          await redis.rPush(`chat:messages:${roomId}`, JSON.stringify(leaveMsg));
          const participantArr = await getParticipantArr(roomId);
          io.to(roomId).emit('participantsUpdate', participantArr);
          io.to(roomId).emit('message', leaveMsg);
        }
      }
    });

    // 참가자 정보 Redis -> array
    async function getParticipantArr(roomId) {
      const room = await redis.hGetAll(`room:${roomId}`);
      const participants = room && room.participants
        ? JSON.parse(room.participants)
        : [];
      return Promise.all(participants.map(async pid => {
        const u = await redis.hGetAll(`user:${pid}`);
        return u && Object.keys(u).length > 0
          ? { id: u.id || pid, name: u.name, email: u.email, profileImage: u.profileImage || '' }
          : { id: pid, name: '알 수 없음', email: '', profileImage: '' };
      }));
    }

    // AI 멘션 추출
    function extractAIMentions(content) {
      if (!content) return [];
      return (content.match(/@(wayneAI|consultingAI)\b/g) || []).map(x => x.replace('@',''));
    }
  });

  return io;
}

function getSocketIO() {
  if (!ioInstance) throw new Error('Socket.IO 인스턴스가 설정되지 않았습니다.');
  return ioInstance;
}

module.exports = { setSocketIO, getSocketIO };

(() => {
  const DEFAULT_LOST = 'Match ended.';
  const NO_PLAYERS = 'No other players joined this match. Try again.';
  const RTC_CONFIG = { iceServers: [
    { urls: ['stun:stun.l.google.com:19302','stun:stun1.l.google.com:19302'] }
  ], ...(window.PLAYVORA_GAMES?.rtcConfig || {}) };
  const STATE_QUEUE_LIMIT = 256, INPUT_QUEUE_LIMIT = 256;
  const RECOVERY_MS = 12000;
  class SnapshotBuffer {
    constructor() { this.clear(); }
    clear() {
      this.frames = []; this.offset = null; this.jitter = 0;
      this.arrival = null; this.hostTime = null; this.cursor = null;
      this.phase = ''; this.reset = true;
    }
    push(snap, now) {
      if (!Number.isFinite(snap.ht) || !Number.isFinite(now)) return false;
      const phase = snap.state + ':' + snap.goalId;
      if (phase !== this.phase || (this.arrival !== null && now - this.arrival > .75)) {
        this.clear(); this.phase = phase;
      }
      if (this.hostTime !== null && snap.ht <= this.hostTime) return false;
      const offset = now - snap.ht;
      if (this.offset === null) this.offset = offset;
      else this.offset = Math.min(offset, this.offset + .0005);
      if (this.arrival !== null) {
        const variation = Math.abs((now - this.arrival) - (snap.ht - this.hostTime));
        this.jitter += (Math.min(.15, variation) - this.jitter) * .12;
      }
      this.arrival = now; this.hostTime = snap.ht;
      this.frames.push(snap);
      if (this.frames.length > 32) this.frames.shift();
      return true;
    }
    sample(now, dt) {
      if (!this.frames.length) return null;
      const first = this.frames[0], last = this.frames[this.frames.length - 1];
      const delay = Math.min(.15, .065 + this.jitter * 2);
      const target = Math.max(first.ht, Math.min(last.ht + .08, now - this.offset - delay));
      if (this.cursor === null) this.cursor = target;
      else this.cursor = Math.max(this.cursor, Math.min(target, this.cursor + Math.max(0, dt) * 1.12));
      while (this.frames.length > 2 && this.frames[1].ht <= this.cursor) this.frames.shift();
      let a = this.frames[0], b = a;
      for (let i = 1; i < this.frames.length; i++) {
        b = this.frames[i];
        if (b.ht >= this.cursor) break;
        a = b;
      }
      const alpha = b.ht > a.ht ? Math.max(0, Math.min(1, (this.cursor - a.ht) / (b.ht - a.ht))) : 0;
      const extra = Math.max(0, Math.min(.08, this.cursor - b.ht));
      const reset = this.reset; this.reset = false;
      return { a, b, alpha, extra, reset, time: this.cursor };
    }
  }
  const API = {
    client: null, user: null, room: null, seat: -1, members: [], isHost: false,
    username: 'You', names: new Map(), skin: 'stock', skins: new Map(), onSkin: null,
    peers: new Map(), pendingCandidates: new Map(),
    polling: null, heartbeat: null, signalPolling: null, p2pHeartbeat: null, connectTimer: null,
    searchTimeout: null, startupWatch: null, finalDelivery: null,
    finding: false, starting: false, active: false, ending: false, finished: false,
    lastPacket: 0, readyAt: 0, signalCursor: 0, signalBusy: false,
    snapshotSeq: 0, receivedSnapshotSeq: 0, rtt: .12, lastInput: null, jumpId: 0, signalFailures: 0,
    onFound: null, onStart: null, onSnapshot: null, onInput: null, onLost: null,
    onReplay: null, onReplayAck: null
  };
  const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));
  const errorText = err => (err && err.message) || String(err);
  const sessionKey = 'turboclash-p2p-tab-v1';
  function validUsername(name) { return typeof name === 'string' && /^[A-Za-z0-9_]{3,12}$/.test(name); }
  function receivedName(name, seat) {
    if (Number.isInteger(seat) && seat >= 0 && seat < 6 && validUsername(name)) API.names.set(seat,name);
  }
  function receivedSkin(id, seat) {
    if (Number.isInteger(seat) && seat >= 0 && seat < 6 && typeof id === 'string' && /^[a-z]{2,16}$/.test(id)) { API.skins.set(seat,id); return true; }
    return false;
  }
  function validRoom() { return !!API.room && (API.starting || API.active); }
  function cleanTimers() {
    for (const x of ['polling','heartbeat','signalPolling','startupWatch','p2pHeartbeat','finalDelivery']) {
      if (API[x]) clearInterval(API[x]); API[x] = null;
    }
    for (const x of ['searchTimeout','connectTimer']) {
      if (API[x]) clearTimeout(API[x]); API[x] = null;
    }
  }
  async function connect() {
    const cfg = window.PLAYVORA_GAMES || {};
    if (!/^https:\/\/[^/]+\.supabase\.co$/.test(cfg.url || '') || !(cfg.publishableKey || '').startsWith('sb_publishable_')) {
      throw new Error('Playvora games matchmaking is not configured.');
    }
    if (!window.supabase || !window.supabase.createClient) throw new Error('Could not load online matchmaking. Check your connection.');
    if (!window.RTCPeerConnection) throw new Error('Peer-to-peer play needs a browser with WebRTC support and HTTPS.');
    if (!API.client) {
      let storage;
      try {
        storage = window.sessionStorage;
        const k = '__tc_storage_test__'; storage.setItem(k,'1'); storage.removeItem(k);
      } catch (_) {
        const mem = new Map();
        storage = { getItem:k=>mem.get(k)??null, setItem:(k,v)=>mem.set(k,v), removeItem:k=>mem.delete(k) };
      }
      API.client = window.supabase.createClient(cfg.url,cfg.publishableKey,{
        auth:{ storage,storageKey:sessionKey,persistSession:true,autoRefreshToken:true,detectSessionInUrl:false }
      });
    }
    const result = await API.client.auth.getSession();
    if (result.error) throw result.error;
    let user = result.data && result.data.session && result.data.session.user;
    if (!user) {
      const signIn = await API.client.auth.signInAnonymously();
      if (signIn.error) throw Error('Guest sign-in failed: '+signIn.error.message+'. Enable Anonymous Sign-Ins in Supabase.');
      user = signIn.data.user;
    }
    if (!user) throw Error('Could not sign in to matchmaking.');
    API.user = user;
  }
  async function rpc(name,args={}) {
    const {data,error}=await API.client.rpc(name,args);
    if (error) throw error;
    return data;
  }
  function memberBySeat(seat) { return API.members.find(p=>p.seat===seat); }
  function peerByUser(userId) { return [...API.peers.values()].find(p=>p.userId===userId); }
  async function signal(to,message) {
    if (!validRoom()) return;
    const room = API.room;
    for (let attempt = 0; attempt < 3; attempt++) {
      try {
        if (!validRoom() || API.room !== room) return;
        await rpc('tc_signal_send',{p_room:room,p_to:to,p_message:message});
        return;
      } catch (err) {
        if (attempt === 2) throw err;
        await sleep(250 * (attempt + 1));
      }
    }
  }
  function keepSignalingForRecovery() {
    if (API.signalPolling) clearInterval(API.signalPolling);

    API.signalPolling = validRoom() && !API.finished ? setInterval(pollSignals, 2000) : null;
  }
  function peerConnected(peer) {
    return peer && peer.control && peer.control.readyState === 'open' &&
      peer.state && peer.state.readyState === 'open' && peer.pc &&
      peer.pc.connectionState === 'connected';
  }
  function startHostIfReady() {
    if (!API.isHost || !API.starting || API.active || !validRoom()) return;
    if (API.peers.size !== API.members.length-1 || ![...API.peers.values()].every(peerConnected)) return;
    if (Date.now()-API.readyAt < 3000) return;
    API.starting=false; API.active=true;
    clearTimeout(API.connectTimer); API.connectTimer=null;
    clearInterval(API.startupWatch); API.startupWatch=null;
    keepSignalingForRecovery();
    for (const p of API.peers.values()) sendControl(p,{type:'start',names:Object.fromEntries(API.names),skins:Object.fromEntries(API.skins)});
    if (API.onStart) API.onStart();
  }
  function sendControl(peer,packet) {
    if (peer && peer.control && peer.control.readyState==='open' && peer.control.bufferedAmount < 65536) {
      try { peer.control.send(JSON.stringify(packet)); return true; } catch (_) {}
    }
    return false;
  }
  function startGuest() {
    if (!validRoom() || API.active || !API.starting) return;
    const delay = Math.max(0,3000-(Date.now()-API.readyAt)), room = API.room;
    setTimeout(()=>{
      if (API.room !== room || !validRoom() || API.active || !API.starting) return;
      API.starting=false; API.active=true;
      clearTimeout(API.connectTimer); API.connectTimer=null;
      clearInterval(API.startupWatch); API.startupWatch=null;
      keepSignalingForRecovery();
      API.lastPacket=Date.now();
      if (API.onStart) API.onStart();
    },delay);
  }
  async function restartPeer(peer) {
    if (!validRoom() || !API.isHost || API.finished || peer.restarting ||
        !['stable','have-local-offer'].includes(peer.pc.signalingState) || peer.pc.connectionState === 'closed' ||
        Date.now() - peer.lastRestart < 4000 || peer.restartAttempts >= 3) return;
    peer.restarting = true; peer.lastRestart = Date.now(); peer.restartAttempts++;
    try {
      if (peer.pc.signalingState === 'stable') {
        const offer = await peer.pc.createOffer({iceRestart:true});
        if (!validRoom() || API.finished || API.peers.get(peer.seat) !== peer) return;
        await peer.pc.setLocalDescription(offer);
      }


      await signal(peer.userId,{kind:'offer',sdp:peer.pc.localDescription.sdp,username:API.username});
    } catch (err) {
      console.warn('Peer recovery will retry:', errorText(err));
    } finally { peer.restarting = false; }
  }
  function detectDisconnect(peer) {
    if (!validRoom() || API.ending || API.finished || peer.disconnectTimer) return;
    peer.recoveryTimer = setTimeout(()=>{
      peer.recoveryTimer = null;
      if (!peerConnected(peer)) restartPeer(peer);
    },1000);
    peer.disconnectTimer = setTimeout(()=>{
      peer.disconnectTimer=null;
      if (validRoom() && !API.finished && !peerConnected(peer)) lost(DEFAULT_LOST,'disconnect',peer.seat);
    },RECOVERY_MS);
  }
  function applySnapshot(packet) {
    if (!API.active || API.finished || API.isHost || !packet.data || !Array.isArray(packet.data.cars)) return;
    if (!Number.isInteger(packet.seq) || packet.seq <= API.receivedSnapshotSeq) return;
    API.receivedSnapshotSeq=packet.seq;
    API.lastPacket=Date.now();
    if (API.onSnapshot) API.onSnapshot(packet.data);
  }


  function sendReplay(id, source, team, scorerSeat) {
    if (!API.active || !API.isHost || !source || !Number.isInteger(id)) return;
    const bytes=new Uint8Array(source.buf.buffer,source.buf.byteOffset,source.buf.byteLength);
    let binary='';
    for (let at=0;at<bytes.length;at+=8192)
      binary+=String.fromCharCode(...bytes.subarray(at,at+8192));
    const base64=btoa(binary), chunks=[];
    for (let at=0;at<base64.length;at+=4000) chunks.push(base64.slice(at,at+4000));
    for (const peer of API.peers.values()) {

      const job={id,index:0,chunks,meta:{type:'replay-meta',id,stride:source.stride,
        count:source.count,team,scorerSeat,parts:chunks.length},metaSent:false,started:Date.now()};
      peer.replayJob=job;
      const pump=()=>{
        if (!API.active || API.finished || peer.replayJob!==job || Date.now()-job.started>20000) return;
        if (!peerConnected(peer)) { setTimeout(pump,100); return; }
        if (!job.metaSent) {
          if (!sendControl(peer,job.meta)) { setTimeout(pump,50); return; }
          job.metaSent=true;
        }
        // Bound replay upload pressure so pings, goals and results keep moving.
        while (job.index<chunks.length && peer.control.bufferedAmount<6000) {
          if (!sendControl(peer,{type:'replay-chunk',id,index:job.index,
              value:chunks[job.index]})) { setTimeout(pump,50); return; }
          job.index++;
        }
        if (job.index<chunks.length) setTimeout(pump,20);
      };
      pump();
    }
  }
  function receiveReplay(peer,packet) {
    if (API.isHost || !API.active) return;
    if (packet.type==='replay-meta') {
      if (!Number.isInteger(packet.id) || packet.id<1 || !Number.isInteger(packet.stride) ||
          packet.stride<34 || packet.stride>78 || !Number.isInteger(packet.count) ||
          packet.count<20 || packet.count>600 || !Number.isInteger(packet.parts) ||
          packet.parts<1 || packet.parts>200 || ![0,1].includes(packet.team)) return;
      peer.replayIn={id:packet.id,stride:packet.stride,count:packet.count,
          team:packet.team,scorerSeat:packet.scorerSeat,parts:new Array(packet.parts),received:0};
      return;
    }
    const r=peer.replayIn;
    if (!r || packet.id!==r.id || !Number.isInteger(packet.index) ||
        packet.index<0 || packet.index>=r.parts.length || typeof packet.value!=='string' ||
        packet.value.length>8000 || r.parts[packet.index]!==undefined) return;
    r.parts[packet.index]=packet.value; r.received++;
    if (r.received!==r.parts.length) return;
    peer.replayIn=null;
    try {
      const raw=atob(r.parts.join(''));
      if (raw.length!==r.stride*r.count*4) return;
      const data=new Uint8Array(raw.length);
      for (let i=0;i<raw.length;i++) data[i]=raw.charCodeAt(i);
      const buf=new Float32Array(data.buffer);
      if (API.onReplay) API.onReplay({id:r.id,stride:r.stride,count:r.count,
        team:r.team,scorerSeat:r.scorerSeat,buf});
      sendControl(peer,{type:'replay-ack',id:r.id});
    } catch(err) { console.warn('Discarded invalid replay data',err); }
  }
  function handleData(peer,data) {
    let packet;
    try { packet=JSON.parse(data); } catch (_) { return; }
    if (!packet || typeof packet !== 'object') return;


    if (packet.type==='match-finished-ack') {
      if (API.isHost && API.finished) {
        peer.ackedFinal=true;
        if ([...API.peers.values()].every(p=>p.ackedFinal) && API.finalDelivery) {
          clearInterval(API.finalDelivery); API.finalDelivery=null;
        }
      }
      return;
    }
    if (packet.type==='match-finished') {
      if (API.isHost || !API.active || peer.seat!==0) return;
      if (API.finished) { sendControl(peer,{type:'match-finished-ack'}); return; }
      if (!packet.data || packet.data.state!=='ended' ||
          !Array.isArray(packet.data.cars) || !Array.isArray(packet.data.scores)) return;
      finishMatch();
      if (API.onSnapshot) API.onSnapshot(packet.data);
      sendControl(peer,{type:'match-finished-ack'});
      return;
    }
    if (API.finished) return;
    if (packet.type==='leaving') { lost(DEFAULT_LOST,'disconnect',peer.seat); return; }
    if (packet.type==='match-disconnected' && Number.isInteger(packet.seat) && packet.seat >= 0 && packet.seat < API.members.length) {
      lost(DEFAULT_LOST,'disconnect',packet.seat); return;
    }
    if (packet.type==='start' && !API.isHost) {
      if (packet.names && typeof packet.names==='object') {
        for (const [seat,name] of Object.entries(packet.names)) receivedName(name,Number(seat));
      }
      if (packet.skins && typeof packet.skins==='object') {
        for (const [seat,id] of Object.entries(packet.skins)) receivedSkin(id,Number(seat));
      }
      startGuest(); return;
    }
    if (packet.type==='ping') { sendControl(peer,{type:'pong',t:packet.t}); return; }
    if (packet.type==='pong') {
      const sample = (performance.now() - packet.t) / 1000;
      if (Number.isFinite(sample) && sample > 0 && sample < 3) API.rtt += (sample - API.rtt) * .2;
      return;
    }
    if (!API.isHost && (packet.type==='replay-meta' || packet.type==='replay-chunk')) {
      receiveReplay(peer,packet); return;
    }
    if (API.isHost && API.active && packet.type==='replay-ack') {
      if (Number.isInteger(packet.id) && API.onReplayAck) API.onReplayAck(peer.seat,packet.id);
      return;
    }
    if (packet.type==='skin') {
      if (API.isHost && receivedSkin(packet.skin,peer.seat) && API.onSkin) API.onSkin(peer.seat,packet.skin);
      return;
    }
    if (API.isHost && API.active && packet.type==='input') {
      if (packet.seat!==peer.seat || !Array.isArray(packet.controls) || packet.controls.length!==7 ||
          packet.controls.some(x=>!Number.isFinite(x) || Math.abs(x)>1.001) ||
          !Number.isSafeInteger(packet.seq) || packet.seq<=peer.inputSeq) return;
      peer.inputSeq=packet.seq;
      const jumpId=Number.isSafeInteger(packet.jumpId) && packet.jumpId>=0 ? packet.jumpId : peer.jumpId;
      const jumpPressed=jumpId>peer.jumpId;
      peer.jumpId=Math.max(peer.jumpId,jumpId);
      if (API.onInput) API.onInput({seat:peer.seat,controls:packet.controls,seq:packet.seq,jumpPressed});
    } else if (!API.isHost && packet.type==='snapshot') applySnapshot(packet);
  }
  function bindChannel(peer,channel) {
    if (channel.label==='control') peer.control=channel;
    else if (channel.label==='state') peer.state=channel;
    else { channel.close(); return; }
    channel.onopen=()=>{
      peer.lastSeen=Date.now();
      if (channel.label==='control' && !API.isHost) sendControl(peer,{type:'skin',skin:API.skin});
      if (peerConnected(peer)) {
        clearTimeout(peer.disconnectTimer); clearTimeout(peer.recoveryTimer);
        peer.disconnectTimer=peer.recoveryTimer=null;
      }
      startHostIfReady();
    };
    channel.onclose=()=>detectDisconnect(peer);
    channel.onerror=()=>detectDisconnect(peer);
    channel.onmessage=ev=>{
      peer.lastSeen=Date.now();
      // Any host traffic, including reliable heartbeat/replay packets, proves
      // the connection is alive even when live snapshots are backpressured.
      if (!API.isHost && API.active && peer.seat===0) API.lastPacket=peer.lastSeen;
      handleData(peer,ev.data);
    };
  }
  function makePeer(member,makeOffer) {
    if (API.peers.has(member.seat)) return API.peers.get(member.seat);
    const peer={seat:member.seat,userId:member.user_id,pc:new RTCPeerConnection(RTC_CONFIG),
      control:null,state:null,disconnectTimer:null,recoveryTimer:null,lastSeen:Date.now(),
      lastRestart:0,restartAttempts:0,restarting:false,inputSeq:-1,jumpId:0,lastPhase:''};
    API.peers.set(member.seat,peer);
    peer.pc.onicecandidate=ev=>{
      if (ev.candidate && validRoom()) signal(peer.userId,{kind:'candidate',candidate:ev.candidate.toJSON()})
        .catch(err=>{ if(validRoom()) console.warn('ICE candidate signaling will recover:',errorText(err)); });
    };
    peer.pc.onconnectionstatechange=()=>{
      if (!validRoom()) return;
      if (peer.pc.connectionState==='connected') {
        clearTimeout(peer.disconnectTimer); clearTimeout(peer.recoveryTimer);
        peer.disconnectTimer=peer.recoveryTimer=null;
        peer.restartAttempts=0; peer.lastSeen=Date.now();
        startHostIfReady();
      } else if (peer.pc.connectionState==='failed' || peer.pc.connectionState==='disconnected' || peer.pc.connectionState==='closed') {
        detectDisconnect(peer);
      }
    };
    if (makeOffer) {
      bindChannel(peer,peer.pc.createDataChannel('control'));
      bindChannel(peer,peer.pc.createDataChannel('state',{ordered:false,maxRetransmits:0}));
    } else {
      peer.pc.ondatachannel=event=>bindChannel(peer,event.channel);
    }
    return peer;
  }
  async function flushCandidates(peer) {
    if (!peer.pc.remoteDescription) return;
    const queued=API.pendingCandidates.get(peer.userId) || [];
    API.pendingCandidates.delete(peer.userId);
    for (const candidate of queued) {
      try { await peer.pc.addIceCandidate(candidate); } catch(err) { console.warn('Ignored invalid ICE candidate',err); }
    }
  }
  async function processSignal(signalRow) {
    const data=signalRow.message;
    if (!data || !['offer','answer','candidate'].includes(data.kind)) return;
    const member=API.members.find(x=>x.user_id===signalRow.sender);
    if (!member || member.seat===API.seat) return;
    if (data.kind==='offer' || data.kind==='answer') receivedName(data.username,member.seat);
    if (API.isHost && member.seat===0) return;
    if (!API.isHost && member.seat!==0) return;
    if (data.kind==='candidate') {
      const peer=peerByUser(signalRow.sender);
      if (!peer || !peer.pc.remoteDescription || peer.pc.signalingState==='have-local-offer') {
        const queued=API.pendingCandidates.get(signalRow.sender)||[];
        if (queued.length<64) queued.push(data.candidate);
        API.pendingCandidates.set(signalRow.sender,queued);
      } else {
        try { await peer.pc.addIceCandidate(data.candidate); }
        catch(_) {
          const queued=API.pendingCandidates.get(signalRow.sender)||[];
          if (queued.length<64) queued.push(data.candidate);
          API.pendingCandidates.set(signalRow.sender,queued);
        }
      }
      return;
    }
    if (data.kind==='offer' && !API.isHost) {
      const peer=makePeer(member,false);
      await peer.pc.setRemoteDescription({type:'offer',sdp:data.sdp});
      await flushCandidates(peer);
      const answer=await peer.pc.createAnswer();
      await peer.pc.setLocalDescription(answer);
      await signal(peer.userId,{kind:'answer',sdp:peer.pc.localDescription.sdp,username:API.username});
    } else if (data.kind==='answer' && API.isHost) {
      const peer=peerByUser(signalRow.sender);
      if (peer && peer.pc.signalingState==='have-local-offer') {
        await peer.pc.setRemoteDescription({type:'answer',sdp:data.sdp});
        await flushCandidates(peer);
      }
    }
  }
  async function pollSignals() {
    if (!validRoom() || API.signalBusy) return;
    API.signalBusy=true;
    const room=API.room;
    try {
      const messages=await rpc('tc_signal_poll',{p_after:API.signalCursor});
      if (!validRoom() || API.room!==room) return;
      API.signalFailures=0;
      for (const m of messages || []) {
        if (m.id<=API.signalCursor) continue;
        await processSignal(m);
        API.signalCursor=m.id;
      }
    } catch(err) {
      if(validRoom() && API.room===room && ++API.signalFailures>=6 && !API.active)
        lost('Peer connection setup failed: '+errorText(err),'error');
    } finally { API.signalBusy=false; }
  }
  async function connectPeers() {
    if (!validRoom()) return;
    API.connectTimer=setTimeout(()=>{
      if (validRoom() && !API.active) lost('Could not establish a direct player connection. Some networks need a TURN relay.','error');
    },45000);
    API.startupWatch=setInterval(startHostIfReady,180);
    API.p2pHeartbeat=setInterval(()=>{
      if (!API.active || API.ending || API.finished) return;
      const now=Date.now();
      for (const peer of API.peers.values()) {
        if (now-peer.lastSeen>RECOVERY_MS+2000) { lost(DEFAULT_LOST,'disconnect',peer.seat); return; }
        if (now-peer.lastSeen>4000 || !peerConnected(peer)) restartPeer(peer);
        sendControl(peer,{type:'ping',t:performance.now()});
      }
    },1000);
    API.signalPolling=setInterval(pollSignals,650);
    await pollSignals();
    if (!validRoom()) return;
    if (API.isHost) {
      for (const member of API.members) {
        if (member.seat===API.seat) continue;
        const peer=makePeer(member,true);
        const offer=await peer.pc.createOffer();
        await peer.pc.setLocalDescription(offer);
        await signal(peer.userId,{kind:'offer',sdp:peer.pc.localDescription.sdp,username:API.username});
      }
    }
  }
  async function search(settings, onSearching = null) {
    await cancel();
    API.finding=true; API.starting=false; API.active=false; API.ending=false; API.finished=false;
    try {
      await connect();
      if (!API.finding) return;
      await rpc('tc_leave');
      if (!API.finding) return;
      const ticket=await rpc('tc_join',{p_mode:settings.mode,p_duration:settings.len,p_skill:settings.diff});
      if (!API.finding) { API.room=ticket.room_id; await cancel(); return; }
      API.room=ticket.room_id; API.seat=ticket.seat; API.isHost=ticket.seat===0;
      API.members=[]; API.signalCursor=0; API.snapshotSeq=0; API.receivedSnapshotSeq=0;
      API.lastInput=null; API.jumpId=0; API.signalFailures=0; API.rtt=.12;
      API.names.clear(); receivedName(API.username,API.seat);
      API.skins.clear(); receivedSkin(API.skin,API.seat);
      API.heartbeat=setInterval(async()=>{
        if (!API.room || API.finished) return;
        try {
          const st=await rpc('tc_status');
          if (API.room && !API.finished && (st.status==='none' || st.status==='cancelled')) {
            lost(API.finding ? NO_PLAYERS : DEFAULT_LOST, API.finding ? 'error' : 'disconnect');
          }
        } catch(_) { }
      },8000);
      const poll=async()=>{
        if (!API.finding) return;
        try {
          const st=await rpc('tc_status');
          if (!API.finding) return;
          if (!st || typeof st.status !== 'string') throw new Error('Invalid matchmaking response.');
          if (st.status==='none' || st.status==='cancelled') return lost(NO_PLAYERS,'error');

          if (onSearching) { const ready=onSearching; onSearching=null; ready(); }
          if (st.status==='ready' && Array.isArray(st.players) && st.players.length===settings.mode*2) {
            API.members=st.players;
            API.finding=false; API.starting=true; API.readyAt=Date.now();
            clearInterval(API.polling); API.polling=null;
            clearTimeout(API.searchTimeout); API.searchTimeout=null;
            if (API.onFound) API.onFound();
            await connectPeers();
          }
        } catch(err) { if(API.finding) lost('Matchmaking failed: '+errorText(err),'error'); }
      };
      API.polling=setInterval(poll,1100);
      API.searchTimeout=setTimeout(()=>{if(API.finding)lost(NO_PLAYERS,'error');},30000);
      await poll();
    } catch(err) {
      await cancel();
      throw err;
    }
  }
  function finishMatch(snapshot=null) {
    if (!API.active || API.finished) return;

    if (API.isHost && snapshot && snapshot.state==='ended') {
      const room=API.room, start=Date.now();
      for (const peer of API.peers.values()) peer.ackedFinal=false;
      const deliver=()=>{
        if (API.room!==room || Date.now()-start>10000 ||
            [...API.peers.values()].every(peer=>peer.ackedFinal)) {
          if (API.finalDelivery) clearInterval(API.finalDelivery);
          API.finalDelivery=null;
          return;
        }
        for (const peer of API.peers.values())
          if (!peer.ackedFinal) sendControl(peer,{type:'match-finished',data:snapshot});
      };
      deliver();
      API.finalDelivery=setInterval(deliver,400);
    }
    API.finished=true;
    for (const timer of ['p2pHeartbeat','heartbeat','signalPolling']) {
      if (API[timer]) clearInterval(API[timer]);
      API[timer]=null;
    }
    for (const peer of API.peers.values()) {
      clearTimeout(peer.disconnectTimer); clearTimeout(peer.recoveryTimer);
      peer.disconnectTimer=peer.recoveryTimer=null;
    }
  }
  function sendInput(controls, seq) {
    if (!API.active || API.finished || API.isHost) return;
    const host=API.peers.get(0);
    if (!host) return;
    if (controls[4] && !API.lastInput?.[4]) API.jumpId++;
    const message={type:'input',seat:API.seat,controls,seq,jumpId:API.jumpId}, packet=JSON.stringify(message);
    if (host.state && host.state.readyState==='open' && host.state.bufferedAmount<INPUT_QUEUE_LIMIT) {
      try { host.state.send(packet); } catch (_) {}
    }


    const edge=!API.lastInput || controls.slice(4).some((v,i)=>v!==API.lastInput[i+4]) ||
      (controls.every(v=>v===0) && API.lastInput.some(v=>v!==0));
    if (edge && host.control?.bufferedAmount<2048) sendControl(host,message);
    API.lastInput=controls.slice();
  }
  function sendSnapshot(snapshot) {
    if (!API.active || API.finished || !API.isHost) return;
    const message={type:'snapshot',seq:++API.snapshotSeq,data:snapshot};


    const packet=JSON.stringify(message,(key,v)=>typeof v==='number' && !Number.isInteger(v) ? Math.round(v*10000)/10000 : v);
    const phase=snapshot.state+':'+snapshot.goalId;
    let phasePacket=null;
    for (const peer of API.peers.values()) {
      if (peer.state && peer.state.readyState==='open' && peer.state.bufferedAmount<STATE_QUEUE_LIMIT) {
        try { peer.state.send(packet); } catch (_) { detectDisconnect(peer); }
      }


      if (peer.lastPhase!==phase) {
        if (!phasePacket) phasePacket=JSON.parse(packet);
        if (sendControl(peer,phasePacket)) peer.lastPhase=phase;
      }
    }
  }
  async function cancel() {
    API.finding=false; API.starting=false; API.active=false;
    cleanTimers();
    if (!API.finished) for (const peer of API.peers.values()) sendControl(peer,{type:'leaving'});
    if (API.peers.size) await sleep(100);
    for (const peer of API.peers.values()) {
      clearTimeout(peer.disconnectTimer); clearTimeout(peer.recoveryTimer);
      peer.replayJob=null;
      try { peer.pc.close(); } catch(_) {}
    }
    API.peers.clear(); API.pendingCandidates.clear(); API.names.clear(); API.skins.clear();
    API.signalBusy=false; API.signalCursor=0;
    API.finished=false;
    const room=API.room;
    API.room=null; API.seat=-1; API.members=[]; API.isHost=false;
    if (room && API.client) await rpc('tc_leave').catch(()=>{});
  }
  async function lost(message,reason='error',disconnectedSeat=null) {
    if (API.finished || API.ending || (!API.room && !API.finding && !API.starting && !API.active)) return;
    API.ending=true;
    if (API.active && reason==='disconnect' && Number.isInteger(disconnectedSeat)) {
      for (const peer of API.peers.values())
        if (peer.seat!==disconnectedSeat) sendControl(peer,{type:'match-disconnected',seat:disconnectedSeat});
    }
    const cb=API.onLost;
    try { if (cb) cb(message,reason,disconnectedSeat); }
    finally { await cancel(); }
  }
  API.setUsername=name=>{
    if (!validUsername(name)) throw new Error('Invalid username');
    API.username=name;
  };
  API.setSkin=id=>{ if (typeof id==='string' && /^[a-z]{2,16}$/.test(id)) API.skin=id; };
  API.SnapshotBuffer=SnapshotBuffer;
  API.search=search; API.cancel=cancel;
  API.sendInput=sendInput; API.sendSnapshot=sendSnapshot; API.sendReplay=sendReplay; API.finishMatch=finishMatch; API.lost=lost;
  window.TCMultiplayer=API;
})();

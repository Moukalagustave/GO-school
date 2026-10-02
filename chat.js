// ==========================================
// chat.js — Go-school ChatManager
// Invariants I1-I10 inchangés.
// Accusés Envoyé / Distribué / Lu, watchdog anti-blocage (15s),
// frappe en cours, sons (sent/received/failed/message_read).
// ==========================================

const OUTBOX_KEY = 'goschool_outbox_v1';
const MAX_OUTBOX = 50;
const MAX_CONTENT_LENGTH = 2000;
const SEND_TIMEOUT_MS = 15000;

const profileCache = new Map();
let typingHideTimeout = null;

function isValidAvatarUrl(url) {
  return typeof url === 'string' && (
    url.startsWith('https://api.dicebear.com/') ||
    url.includes('/storage/v1/object/public/avatars/')
  );
}

async function getProfile(userId) {
  if (profileCache.has(userId)) return profileCache.get(userId);
  const { data } = await supabaseClient
    .from('profiles').select('id, prenom, avatar_url').eq('id', userId).maybeSingle();
  const value = data || { id: userId, prenom: 'Élève', avatar_url: null };
  profileCache.set(userId, value);
  return value;
}

function showTypingIndicator() {
  const el = document.getElementById('typing-indicator');
  if (!el) return;
  el.textContent = 'en train d\u2019écrire…';
  el.style.display = 'block';
  clearTimeout(typingHideTimeout);
  typingHideTimeout = setTimeout(() => { el.style.display = 'none'; }, 3000);
}

// ---------- SONS (une seule définition, noms alignés sur /sounds/*.wav) ----------
const soundCache = {};
function playSound(name) {
  try {
    if (!soundCache[name]) soundCache[name] = new Audio(`sounds/${name}.wav`);
    soundCache[name].currentTime = 0;
    // Les navigateurs bloquent l'autoplay avant toute interaction utilisateur —
    // échec silencieux attendu, jamais bloquant pour le chat lui-même.
    soundCache[name].play().catch(() => {});
  } catch (e) { /* non critique */ }
}

function withTimeout(promise, ms) {
  return Promise.race([
    promise,
    new Promise((_, reject) => setTimeout(() => reject(new Error('TIMEOUT')), ms))
  ]);
}

// ---------- SONS ----------
// Échec silencieux si l'autoplay est bloqué par le navigateur — jamais d'erreur affichée à l'utilisateur.
const SOUNDS = {
  sent: 'sounds/sent.wav',
  received: 'sounds/received.wav',
  failed: 'sounds/failed.wav',
  message_read: 'sounds/message_read.wav',
  friend_online: 'sounds/friend_online.wav'
};
function playSound(name) {
  const src = SOUNDS[name];
  if (!src) return;
  try {
    new Audio(src).play().catch(() => {});
  } catch (e) { /* ignore */ }
}

class ChatManager {
  constructor(conversationId, currentUserId, isGroup = false) {
    this.conversationId = conversationId;
    this.currentUserId = currentUserId;
    this.isGroup = isGroup;
    this.state = 'DISCONNECTED';
    this.generation = 0;
    this.channel = null;
    this.reconnectTimer = null;
    this.reconnectDelays = [2000, 5000, 10000];
    this.reconnectAttempt = 0;
    this.lastKnownSeq = 0;
    this.stopped = false;

    this.otherReadSeq = 0;
    this.otherDeliveredSeq = 0;
    this.readSoundPlayedFor = new Set();

    this._onVisibilityChange = () => { if (document.visibilityState === 'visible') this.markAsRead(); };
    document.addEventListener('visibilitychange', this._onVisibilityChange);
  }

  setState(next) {
    this.state = next;
    renderConnectionStatus(next);
  }

  async start() {
    this.stopped = false;
    await this.connect();
  }

  stop() {
    this.stopped = true;
    clearTimeout(this.reconnectTimer);
    this.reconnectTimer = null;
    document.removeEventListener('visibilitychange', this._onVisibilityChange);
    if (this.channel) {
      supabaseClient.removeChannel(this.channel);
      this.channel = null;
    }
    this.setState('STOPPED');
  }

  async connect() {
    if (this.stopped) return;
    this.generation += 1;
    const myGen = this.generation;

    if (this.channel) {
      await supabaseClient.removeChannel(this.channel);
      this.channel = null;
    }

    this.setState('CONNECTING');

    const channel = supabaseClient.channel(`conversation:${this.conversationId}:gen${myGen}`);

    channel.on('postgres_changes', {
      event: 'INSERT', schema: 'public', table: 'messages',
      filter: `conversation_id=eq.${this.conversationId}`
    }, (payload) => {
      if (myGen !== this.generation) return;
      this.handleRealtimeInsert(payload.new);
    });

    if (!this.isGroup) {
      channel.on('postgres_changes', {
        event: 'UPDATE', schema: 'public', table: 'conversation_members',
        filter: `conversation_id=eq.${this.conversationId}`
      }, (payload) => {
        if (myGen !== this.generation) return;
        if (payload.new.user_id === this.currentUserId) return;
        const newReadSeq = payload.new.last_read_seq || 0;
        if (newReadSeq > this.otherReadSeq) playSound('message_read'); // transition réelle uniquement
        this.otherReadSeq = newReadSeq;
        this.otherDeliveredSeq = payload.new.last_delivered_seq || 0;
        this.refreshOwnMessageReceipts();
      });
    }

    channel.on('broadcast', { event: 'typing' }, ({ payload }) => {
      if (myGen !== this.generation) return;
      if (payload?.user_id && payload.user_id !== this.currentUserId) showTypingIndicator();
    });

    channel.subscribe(async (status) => {
      if (myGen !== this.generation) return;
      if (status === 'SUBSCRIBED') {
        this.setState('SUBSCRIBED');
        await this.syncAndFlush(myGen);
      } else if (status === 'CHANNEL_ERROR' || status === 'TIMED_OUT' || status === 'CLOSED') {
        this.handleDisconnect(myGen);
      }
    });

    this.channel = channel;
  }

  handleDisconnect(myGen) {
    if (myGen !== this.generation || this.stopped) return;
    this.setState('RECONNECT_WAIT');
    clearTimeout(this.reconnectTimer);
    const delay = this.reconnectDelays[Math.min(this.reconnectAttempt, this.reconnectDelays.length - 1)];
    this.reconnectAttempt += 1;
    this.reconnectTimer = setTimeout(() => {
      if (!this.stopped) this.connect();
    }, delay);
  }

  async syncAndFlush(myGen) {
    this.setState('SYNCING');

    const { data, error } = await supabaseClient
      .from('messages')
      .select('id, conversation_id, sender_id, client_message_id, content, created_at, seq, reported, deleted_for_everyone, reply_to_client_message_id')
      .eq('conversation_id', this.conversationId)
      .gt('seq', this.lastKnownSeq)
      .order('seq', { ascending: true });

    if (myGen !== this.generation) return;

    // NB : ce fetch est une synchronisation historique — aucun son ne doit se jouer ici,
    // "received" ne se déclenche que pour les arrivées EN DIRECT (handleRealtimeInsert).
    if (!error && data) {
      data.forEach(msg => {
        this.renderMessage(msg, 'confirmed');
        if (msg.seq > this.lastKnownSeq) this.lastKnownSeq = msg.seq;
      });
    } else if (error) {
      console.error('Erreur synchronisation REST:', error);
    }

    if (!this.isGroup) {
      const { data: otherMember } = await supabaseClient
        .from('conversation_members')
        .select('user_id, last_read_seq, last_delivered_seq')
        .eq('conversation_id', this.conversationId)
        .neq('user_id', this.currentUserId)
        .maybeSingle();
      if (otherMember) {
        this.otherReadSeq = otherMember.last_read_seq || 0;
        this.otherDeliveredSeq = otherMember.last_delivered_seq || 0;
        this.refreshOwnMessageReceipts();
      }
    }

    this.reconnectAttempt = 0;
    this.setState('READY');
    await this.markAsDelivered();
    await this.markAsRead();
    await this.flushOutbox();
  }

  handleRealtimeInsert(row) {
    this.renderMessage(row, 'confirmed');
    if (row.sender_id !== this.currentUserId) playSound('received'); // un seul appel, ici seulement
    if (row.seq && row.seq > this.lastKnownSeq) this.lastKnownSeq = row.seq;
    if (this.state === 'READY') {
      this.markAsDelivered();
      this.markAsRead();
    }
  }

  // ---------- ACCUSÉS : DISTRIBUÉ vs LU ----------

  async markAsDelivered() {
    if (this.lastKnownSeq <= 0) return;
    await supabaseClient
      .from('conversation_members')
      .update({ last_delivered_seq: this.lastKnownSeq })
      .eq('conversation_id', this.conversationId)
      .eq('user_id', this.currentUserId)
      .lt('last_delivered_seq', this.lastKnownSeq);
  }

  async markAsRead() {
    if (this.lastKnownSeq <= 0 || document.visibilityState !== 'visible') return;
    await supabaseClient
      .from('conversation_members')
      .update({ last_read_seq: this.lastKnownSeq })
      .eq('conversation_id', this.conversationId)
      .eq('user_id', this.currentUserId)
      .lt('last_read_seq', this.lastKnownSeq);
  }

  refreshOwnMessageReceipts() {
    document.querySelectorAll('#chat-messages .chat-message-row.mine').forEach(row => {
      const seq = Number(row.dataset.seq || 0);
      if (!seq) return;
      const statusEl = row.querySelector('.chat-message-status');
      if (!statusEl) return;
      if (this.otherReadSeq >= seq) {
        statusEl.textContent = 'Lu ✓✓';
        if (!this.readSoundPlayedFor.has(seq)) {
          this.readSoundPlayedFor.add(seq);
          playSound('message_read');
        }
      } else if (this.otherDeliveredSeq >= seq) {
        statusEl.textContent = 'Distribué ✓';
      }
    });
  }

  notifyTyping() {
    if (this.channel && this.state === 'READY') {
      this.channel.send({ type: 'broadcast', event: 'typing', payload: { user_id: this.currentUserId } });
    }
  }

  // ---------- OUTBOX ----------

  loadOutbox() {
    try {
      const raw = localStorage.getItem(OUTBOX_KEY);
      return raw ? JSON.parse(raw) : [];
    } catch (e) {
      console.error('Outbox illisible, réinitialisation', e);
      return [];
    }
  }

  saveOutbox(entries) {
    try {
      localStorage.setItem(OUTBOX_KEY, JSON.stringify(entries));
    } catch (e) {
      if (e.name === 'QuotaExceededError') {
        entries = entries.slice(-Math.floor(MAX_OUTBOX / 2));
        try { localStorage.setItem(OUTBOX_KEY, JSON.stringify(entries)); }
        catch (e2) { console.error('Outbox toujours pleine après purge', e2); }
      } else {
        console.error('Échec écriture Outbox', e);
      }
    }
  }

  async sendMessage(content, replyToId = null) {
    content = content.trim();
    if (!content || content.length > MAX_CONTENT_LENGTH) return;

    const outbox = this.loadOutbox();
    if (outbox.length >= MAX_OUTBOX) {
      showToast("File d'attente pleine, attends l'envoi des messages précédents.");
      return;
    }

    const clientMessageId = crypto.randomUUID();
    const entry = {
      client_message_id: clientMessageId,
      conversation_id: this.conversationId,
      sender_id: this.currentUserId,
      content,
      reply_to_client_message_id: replyToId || null,
      created_at_local: new Date().toISOString(),
      status: 'queued'
    };
    outbox.push(entry);
    this.saveOutbox(outbox);

    this.renderMessage(entry, 'pending');
    await this.flushOutbox();
  }

  // ---------- ACTIONS MESSAGE ----------

  async deleteForSelf(clientMessageId, messageId) {
    // messageId = id réel (pas client_message_id) — nécessaire pour la table de masquage
    const { error } = await supabaseClient
      .from('message_hidden_for_user')
      .insert({ message_id: messageId, user_id: this.currentUserId });
    if (error && error.code !== '23505') { console.error('Échec masquage message:', error); return; }
    const row = document.querySelector(`[data-client-id="${cssEscape(clientMessageId)}"]`);
    if (row) row.remove();
  }

  async deleteForEveryone(clientMessageId) {
    const { error } = await supabaseClient
      .from('messages')
      .update({ deleted_for_everyone: true })
      .eq('client_message_id', clientMessageId)
      .eq('sender_id', this.currentUserId);
    if (error) { console.error('Échec suppression pour tous:', error); showToast('Action non autorisée.'); return; }
    const row = document.querySelector(`[data-client-id="${cssEscape(clientMessageId)}"]`);
    if (row) {
      const content = row.querySelector('.chat-message-content');
      if (content) content.textContent = 'Message supprimé';
      row.classList.add('deleted');
    }
  }

  async leaveGroup() {
    const { error } = await supabaseClient
      .from('conversation_members')
      .delete()
      .eq('conversation_id', this.conversationId)
      .eq('user_id', this.currentUserId);
    if (error) { console.error('Échec sortie du groupe:', error); showToast('Erreur, réessaie.'); return false; }
    this.stop();
    return true;
  }

  async flushOutbox() {
    if (this.state !== 'READY') return;
    let outbox = this.loadOutbox();

    for (const entry of outbox) {
      if (entry.status === 'sent') continue;
      entry.status = 'sending';
      this.saveOutbox(outbox);
      renderMessageStatus(entry.client_message_id, 'sending');

      let insertResult;
      try {
        insertResult = await withTimeout(
          supabaseClient.from('messages').insert({
            conversation_id: entry.conversation_id,
            sender_id: entry.sender_id,
            client_message_id: entry.client_message_id,
            content: entry.content,
            reply_to_client_message_id: entry.reply_to_client_message_id || null
          }),
          SEND_TIMEOUT_MS
        );
      } catch (timeoutErr) {
        // Watchdog anti-blocage : l'état optimiste ne reste jamais bloqué indéfiniment.
        // Le message reste en Outbox (pas supprimé) — un prochain flush retentera,
        // le double-check 23505 protège contre un doublon si l'INSERT avait en fait réussi.
        entry.status = 'failed';
        renderMessageStatus(entry.client_message_id, 'failed');
        playSound('failed');
        continue;
      }

      const { error } = insertResult;

      if (!error) {
        entry.status = 'sent';
        renderMessageStatus(entry.client_message_id, 'sent');
        playSound('sent');
        continue;
      }

      if (error.code === '23505') {
        const { data: confirmRow } = await supabaseClient
          .from('messages')
          .select('sender_id, conversation_id, client_message_id')
          .eq('client_message_id', entry.client_message_id)
          .maybeSingle();

        const matches = confirmRow
          && confirmRow.sender_id === entry.sender_id
          && confirmRow.conversation_id === entry.conversation_id;

        entry.status = matches ? 'sent' : 'failed';
        renderMessageStatus(entry.client_message_id, entry.status);
        // Pas de son ici : ce n'est qu'une réconciliation de retry, pas un nouvel envoi.
        continue;
      }

      console.error('Rejet sécurité messages:', error);
      entry.status = 'failed';
      renderMessageStatus(entry.client_message_id, 'failed');
      playSound('failed');
    }

    outbox = outbox.filter(e => e.status !== 'sent');
    this.saveOutbox(outbox);
  }

  // ---------- RENDU / DÉDUPLICATION (I5, XSS) ----------

  renderMessage(msg, kind) {
    const id = msg.client_message_id;
    if (!id) return;
    const isMine = msg.sender_id === this.currentUserId;

    let row = document.querySelector(`[data-client-id="${cssEscape(id)}"]`);
    if (!row) {
      row = document.createElement('div');
      row.className = 'chat-message-row' + (isMine ? ' mine' : '');
      row.dataset.clientId = id;
      if (msg.seq) row.dataset.seq = msg.seq;

      if (this.isGroup && !isMine) {
        const avatarImg = document.createElement('img');
        avatarImg.className = 'chat-message-avatar';
        avatarImg.alt = '';
        row.appendChild(avatarImg);
        getProfile(msg.sender_id).then(p => {
          avatarImg.src = isValidAvatarUrl(p.avatar_url)
            ? p.avatar_url
            : `https://api.dicebear.com/9.x/bottts/svg?seed=${encodeURIComponent(msg.sender_id)}`;
        });
      }

      const bubble = document.createElement('div');
      bubble.className = 'chat-message' + (isMine ? ' mine' : '');

      if (this.isGroup && !isMine) {
        const senderEl = document.createElement('span');
        senderEl.className = 'chat-message-sender';
        bubble.appendChild(senderEl);
        getProfile(msg.sender_id).then(p => { senderEl.textContent = p.prenom || 'Élève'; });
      }

      if (msg.reply_to_client_message_id) {
        const quoted = document.querySelector(`[data-client-id="${cssEscape(msg.reply_to_client_message_id)}"] .chat-message-content`);
        const quote = document.createElement('div');
        quote.className = 'chat-message-quote';
        quote.textContent = quoted ? quoted.textContent : 'Message';
        bubble.appendChild(quote);
      }

      const content = document.createElement('span');
      content.className = 'chat-message-content';
      content.textContent = msg.deleted_for_everyone ? 'Message supprimé' : msg.content;
      if (msg.deleted_for_everyone) bubble.classList.add('deleted');
      bubble.appendChild(content);

      // Menu d'actions — visible au clic sur la bulle, pas de suppression possible sur un message déjà effacé
      if (!msg.deleted_for_everyone && msg.id) {
        const actions = document.createElement('div');
        actions.className = 'chat-message-actions';

        const replyBtn = document.createElement('button');
        replyBtn.type = 'button';
        replyBtn.textContent = 'Répondre';
        replyBtn.addEventListener('click', (e) => { e.stopPropagation(); if (window.setReplyTarget) window.setReplyTarget(id, msg.content); });
        actions.appendChild(replyBtn);

        if (isMine) {
          const delAllBtn = document.createElement('button');
          delAllBtn.type = 'button';
          delAllBtn.textContent = 'Supprimer pour tous';
          delAllBtn.addEventListener('click', (e) => { e.stopPropagation(); this.deleteForEveryone(id); });
          actions.appendChild(delAllBtn);
        }
        const delSelfBtn = document.createElement('button');
        delSelfBtn.type = 'button';
        delSelfBtn.textContent = 'Supprimer pour moi';
        delSelfBtn.addEventListener('click', (e) => { e.stopPropagation(); this.deleteForSelf(id, msg.id); });
        actions.appendChild(delSelfBtn);
        bubble.appendChild(actions);
        bubble.addEventListener('click', () => bubble.classList.toggle('actions-open'));
      }

      const meta = document.createElement('div');
      meta.className = 'chat-message-meta';

      const timeEl = document.createElement('span');
      timeEl.className = 'chat-message-time';
      const ts = msg.created_at || msg.created_at_local;
      timeEl.textContent = ts
        ? new Date(ts).toLocaleTimeString('fr-FR', { hour: '2-digit', minute: '2-digit' })
        : '';
      meta.appendChild(timeEl);

      const status = document.createElement('span');
      status.className = 'chat-message-status';
      meta.appendChild(status);

      bubble.appendChild(meta);
      row.appendChild(bubble);
      document.getElementById('chat-messages').appendChild(row);
      row.scrollIntoView({ block: 'nearest' });
    } else if (msg.seq) {
      row.dataset.seq = msg.seq;
    }
    this.updateStatusNode(row, kind === 'confirmed' ? 'sent' : kind);
    if (isMine && msg.seq) this.refreshOwnMessageReceipts();
  }

  updateStatusNode(row, status) {
    const statusEl = row.querySelector('.chat-message-status');
    const bubbleEl = row.querySelector('.chat-message');
    const labels = { pending: 'En attente…', sending: 'Envoi…', sent: '', failed: 'Échec — réessaie' };
    if (statusEl) statusEl.textContent = labels[status] ?? '';
    if (bubbleEl) bubbleEl.classList.toggle('failed', status === 'failed');
  }
}

function renderMessageStatus(clientId, status) {
  const row = document.querySelector(`[data-client-id="${cssEscape(clientId)}"]`);
  if (!row) return;
  const labels = { sending: 'Envoi…', sent: '', failed: 'Échec — réessaie' };
  const statusEl = row.querySelector('.chat-message-status');
  const bubbleEl = row.querySelector('.chat-message');
  if (statusEl) statusEl.textContent = labels[status] ?? '';
  if (bubbleEl) bubbleEl.classList.toggle('failed', status === 'failed');
}

function renderConnectionStatus(state) {
  const el = document.getElementById('connection-status');
  const labels = {
    DISCONNECTED: 'Déconnecté', CONNECTING: 'Connexion…', SUBSCRIBED: 'Connecté',
    SYNCING: 'Synchronisation…', READY: 'En ligne', RECONNECT_WAIT: 'Reconnexion…', STOPPED: 'Hors ligne'
  };
  if (el) el.textContent = labels[state] ?? state;
}

function cssEscape(str) {
  return window.CSS && CSS.escape ? CSS.escape(str) : str.replace(/[^a-zA-Z0-9_-]/g, '\\$&');
}

function showToast(msg) {
  const t = document.getElementById('toast');
  if (!t) { alert(msg); return; }
  t.textContent = msg;
  t.style.display = 'block';
  clearTimeout(showToast._t);
  showToast._t = setTimeout(() => { t.style.display = 'none'; }, 3000);
}

// ---------- CRÉATION DE CONVERSATIONS ----------

async function createDirectConversation(otherUserId, currentUserId) {
  const { data: conv, error } = await supabaseClient
    .from('conversations')
    .insert({ type: 'direct', created_by: currentUserId, direct_user_a: currentUserId, direct_user_b: otherUserId })
    .select('id')
    .single();

  if (error) {
    if (error.code === '23505') {
      const { data: existing } = await supabaseClient
        .from('conversations')
        .select('id')
        .eq('type', 'direct')
        .or(`and(direct_user_a.eq.${currentUserId},direct_user_b.eq.${otherUserId}),and(direct_user_a.eq.${otherUserId},direct_user_b.eq.${currentUserId})`)
        .maybeSingle();
      return existing ? existing.id : null;
    }
    console.error('Création conversation directe échouée (probable : pas encore amis)', error);
    return null;
  }

  await supabaseClient.from('conversation_members').insert([
    { conversation_id: conv.id, user_id: currentUserId, role: 'admin' },
    { conversation_id: conv.id, user_id: otherUserId, role: 'member' }
  ]);
  return conv.id;
}

async function createGroupConversation(name, memberIds, currentUserId) {
  const { data: conv, error } = await supabaseClient
    .from('conversations')
    .insert({ type: 'group', name, created_by: currentUserId })
    .select('id')
    .single();

  if (error) { console.error('Création groupe échouée', error); return null; }

  const rows = [{ conversation_id: conv.id, user_id: currentUserId, role: 'admin' }]
    .concat(memberIds.filter(id => id !== currentUserId).map(id => ({ conversation_id: conv.id, user_id: id, role: 'member' })));

  await supabaseClient.from('conversation_members').insert(rows);
  return conv.id;
}

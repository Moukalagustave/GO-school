// ==========================================
// chat.js — Go-school ChatManager
// Invariants I1-I10 inchangés.
// Accusés Envoyé / Distribué / Lu, watchdog anti-blocage (15s),
// frappe en cours, sons (sent/received/failed/message_read/friend_online).
// Ajouts de cette version :
//  - pagination (30 messages + scroll infini vers le haut)
//  - READY uniquement si la synchro REST initiale réussit
//  - gardes generation/stopped après chaque await
//  - images via bucket PRIVÉ chat-media (URLs signées)
//  - Outbox : plus d'écrasement par un snapshot périmé, flush non réentrant
// ==========================================

const OUTBOX_KEY = 'goschool_outbox_v1';
const MAX_OUTBOX = 50;
const MAX_CONTENT_LENGTH = 2000;
const SEND_TIMEOUT_MS = 15000;
const UPLOAD_TIMEOUT_MS = 60000;
const PAGE_SIZE = 30;
const SCROLL_LOAD_THRESHOLD_PX = 80;

const SUPABASE_HOST = 'afwutxnapjrauouazcui.supabase.co';
const MEDIA_BUCKET = 'chat-media';
const MAX_MEDIA_SIZE = 5 * 1024 * 1024; // le vrai plafond doit être posé côté bucket
const MEDIA_EXT = { 'image/jpeg': 'jpg', 'image/png': 'png', 'image/webp': 'webp' };
const MEDIA_PLACEHOLDER = '📷 Photo';
const SIGNED_URL_TTL_S = 3600;

const MESSAGE_COLUMNS =
  'id, conversation_id, sender_id, client_message_id, content, created_at, seq, reported, ' +
  'deleted_for_everyone, reply_to_client_message_id, media_url, media_type';

const profileCache = new Map();
const signedUrlCache = new Map();
let typingHideTimeout = null;

// ---------- VALIDATION D'URL / CHEMINS (filtres d'affichage ; l'autorité reste RLS + Storage) ----------

function isValidAvatarUrl(url) {
  try {
    const u = new URL(url);
    if (u.protocol !== 'https:') return false;
    if (u.hostname === 'api.dicebear.com') return true;
    return u.hostname === SUPABASE_HOST && u.pathname.startsWith('/storage/v1/object/public/avatars/');
  } catch (e) {
    return false;
  }
}

// Chemin attendu dans le bucket privé : {conversation_id}/{client_message_id}.{jpg|png|webp}
function isValidMediaPath(path, conversationId) {
  if (typeof path !== 'string') return false;
  const m = /^([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})\/([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})\.(jpg|png|webp)$/i.exec(path);
  return !!m && m[1].toLowerCase() === String(conversationId).toLowerCase();
}

async function getSignedMediaUrl(path) {
  const cached = signedUrlCache.get(path);
  if (cached && cached.expiresAt > Date.now() + 60000) return cached.url;
  const { data, error } = await supabaseClient.storage
    .from(MEDIA_BUCKET)
    .createSignedUrl(path, SIGNED_URL_TTL_S);
  if (error || !data || !data.signedUrl) {
    console.error('Échec URL signée média:', error);
    return null;
  }
  signedUrlCache.set(path, { url: data.signedUrl, expiresAt: Date.now() + SIGNED_URL_TTL_S * 1000 });
  return data.signedUrl;
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

function withTimeout(promise, ms) {
  return Promise.race([
    promise,
    new Promise((_, reject) => setTimeout(() => reject(new Error('TIMEOUT')), ms))
  ]);
}

// ---------- SONS (une seule définition) ----------
// Échec silencieux si l'autoplay est bloqué par le navigateur — jamais d'erreur affichée.
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

    // Pagination
    this.oldestLoadedSeq = null;
    this.hasMoreOlder = false;
    this.loadingOlder = false;
    this._onScroll = null;
    this._scrollEl = null;

    // Outbox
    this.flushing = false;
    this.flushQueued = false;

    this.blobUrls = [];

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
    this._detachScrollListener();
    this.blobUrls.forEach(u => URL.revokeObjectURL(u));
    this.blobUrls = [];
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
      if (myGen !== this.generation || this.stopped) return;
    }

    this.setState('CONNECTING');

    const channel = supabaseClient.channel(`conversation:${this.conversationId}:gen${myGen}`);

    channel.on('postgres_changes', {
      event: 'INSERT', schema: 'public', table: 'messages',
      filter: `conversation_id=eq.${this.conversationId}`
    }, (payload) => {
      if (myGen !== this.generation || this.stopped) return;
      this.handleRealtimeInsert(payload.new);
    });

    if (!this.isGroup) {
      channel.on('postgres_changes', {
        event: 'UPDATE', schema: 'public', table: 'conversation_members',
        filter: `conversation_id=eq.${this.conversationId}`
      }, (payload) => {
        if (myGen !== this.generation || this.stopped) return;
        if (payload.new.user_id === this.currentUserId) return;
        const newReadSeq = payload.new.last_read_seq || 0;
        if (newReadSeq > this.otherReadSeq) playSound('message_read'); // transition réelle uniquement
        this.otherReadSeq = newReadSeq;
        this.otherDeliveredSeq = payload.new.last_delivered_seq || 0;
        this.refreshOwnMessageReceipts();
      });
    }

    channel.on('broadcast', { event: 'typing' }, ({ payload }) => {
      if (myGen !== this.generation || this.stopped) return;
      if (payload?.user_id && payload.user_id !== this.currentUserId) showTypingIndicator();
    });

    channel.subscribe(async (status) => {
      if (myGen !== this.generation || this.stopped) return;
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

  // ---------- SYNCHRO INITIALE / RATTRAPAGE ----------
  // Premier chargement (lastKnownSeq = 0) : les PAGE_SIZE derniers messages.
  // Reconnexion (lastKnownSeq > 0) : rattrapage de tout ce qui a un seq supérieur, sans trou.
  // Invariant : READY n'est atteint que si cette requête REST a réussi.

  async syncAndFlush(myGen) {
    this.setState('SYNCING');

    const initial = this.lastKnownSeq === 0;
    let query = supabaseClient
      .from('messages')
      .select(MESSAGE_COLUMNS)
      .eq('conversation_id', this.conversationId);
    query = initial
      ? query.order('seq', { ascending: false }).limit(PAGE_SIZE)
      : query.gt('seq', this.lastKnownSeq).order('seq', { ascending: true });

    const { data, error } = await query;
    if (myGen !== this.generation || this.stopped) return;

    if (error) {
      console.error('Erreur synchronisation REST:', error);
      this.handleDisconnect(myGen); // pas de READY, pas d'accusés, pas de flush
      return;
    }

    // NB : synchronisation historique — aucun son ici ("received" = arrivées en direct uniquement).
    const rows = data || [];
    const chronological = initial ? rows.slice().reverse() : rows;
    chronological.forEach(msg => {
      this.renderMessage(msg, 'confirmed');
      if (msg.seq > this.lastKnownSeq) this.lastKnownSeq = msg.seq;
    });
    if (initial) {
      if (chronological.length > 0) this.oldestLoadedSeq = chronological[0].seq;
      this.hasMoreOlder = chronological.length === PAGE_SIZE;
    }

    if (!this.isGroup) {
      const { data: otherMember } = await supabaseClient
        .from('conversation_members')
        .select('user_id, last_read_seq, last_delivered_seq')
        .eq('conversation_id', this.conversationId)
        .neq('user_id', this.currentUserId)
        .maybeSingle();
      if (myGen !== this.generation || this.stopped) return;
      if (otherMember) {
        this.otherReadSeq = otherMember.last_read_seq || 0;
        this.otherDeliveredSeq = otherMember.last_delivered_seq || 0;
        this.refreshOwnMessageReceipts();
      }
    }

    this.reconnectAttempt = 0;
    this.setState('READY');
    await this.markAsDelivered();
    if (myGen !== this.generation || this.stopped) return;
    await this.markAsRead();
    if (myGen !== this.generation || this.stopped) return;
    await this.flushOutbox();
    if (myGen !== this.generation || this.stopped) return;
    this._attachScrollListener();
    this._maybeFillViewport();
  }

  // ---------- PAGINATION VERS LE HAUT ----------

  _attachScrollListener() {
    if (this._onScroll) return;
    const el = document.getElementById('chat-messages');
    if (!el) return;
    this._scrollEl = el;
    this._onScroll = () => {
      if (el.scrollTop < SCROLL_LOAD_THRESHOLD_PX) this.loadOlderMessages();
    };
    el.addEventListener('scroll', this._onScroll, { passive: true });
  }

  _detachScrollListener() {
    if (this._onScroll && this._scrollEl) {
      this._scrollEl.removeEventListener('scroll', this._onScroll);
    }
    this._onScroll = null;
    this._scrollEl = null;
  }

  // Écran haut : 30 messages peuvent ne pas remplir la zone, donc aucun scroll possible.
  _maybeFillViewport() {
    const el = document.getElementById('chat-messages');
    if (!el || this.stopped || el.clientHeight === 0) return;
    if (!this.hasMoreOlder || el.scrollHeight > el.clientHeight + 1) return;
    const before = this.oldestLoadedSeq;
    this.loadOlderMessages().then(() => {
      // Ne récurse que si la page a réellement progressé (pas de boucle sur erreur persistante).
      if (!this.stopped && this.oldestLoadedSeq !== before) this._maybeFillViewport();
    });
  }

  async loadOlderMessages() {
    if (this.loadingOlder || !this.hasMoreOlder || this.oldestLoadedSeq == null) return;
    if (this.stopped || this.state !== 'READY') return;
    const container = document.getElementById('chat-messages');
    if (!container) return;

    const myGen = this.generation;
    this.loadingOlder = true;
    const previousHeight = container.scrollHeight;
    const previousTop = container.scrollTop;

    try {
      const { data, error } = await supabaseClient
        .from('messages')
        .select(MESSAGE_COLUMNS)
        .eq('conversation_id', this.conversationId)
        .lt('seq', this.oldestLoadedSeq)
        .order('seq', { ascending: false })
        .limit(PAGE_SIZE);

      if (myGen !== this.generation || this.stopped) return; // aucun accès DOM
      if (error) {
        console.error('Erreur chargement messages plus anciens:', error);
        return;
      }

      const rows = data || []; // ordre décroissant : chaque insertion en tête reconstitue l'ordre chronologique
      rows.forEach(msg => this.renderMessage(msg, 'confirmed', true));
      if (rows.length > 0) this.oldestLoadedSeq = rows[rows.length - 1].seq;
      this.hasMoreOlder = rows.length === PAGE_SIZE;
      this.refreshOwnMessageReceipts();

      container.scrollTop = previousTop + (container.scrollHeight - previousHeight);
    } finally {
      this.loadingOlder = false;
    }
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
      const parsed = raw ? JSON.parse(raw) : [];
      return Array.isArray(parsed) ? parsed : [];
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

  // Modifie UNE entrée sur l'état courant du stockage (jamais un snapshot périmé).
  patchOutboxEntry(clientMessageId, patch) {
    const entries = this.loadOutbox();
    const target = entries.find(e => e.client_message_id === clientMessageId);
    if (!target) return;
    Object.assign(target, patch);
    this.saveOutbox(entries);
  }

  async sendMessage(content, replyToId = null) {
    if (this.stopped) return;
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

  // ---------- ENVOI D'IMAGE (choix A : upload d'abord, Outbox ensuite) ----------
  // Aucune durabilité tant que l'upload n'a pas réussi : un rechargement pendant l'upload
  // perd l'envoi (limite assumée). Une image n'est jamais "queued" avant que son chemin soit connu.

  async sendMedia(file, caption = '', replyToId = null) {
    if (this.stopped || this.state !== 'READY') {
      showToast('Connexion en cours, réessaie dans un instant.');
      return false;
    }
    const ext = file && MEDIA_EXT[file.type];
    if (!ext) { showToast('Format non pris en charge (JPEG, PNG ou WebP).'); return false; }
    if (file.size > MAX_MEDIA_SIZE) { showToast('Image trop lourde (max 5 Mo).'); return false; }
    if (this.loadOutbox().length >= MAX_OUTBOX) {
      showToast("File d'attente pleine, attends l'envoi des messages précédents.");
      return false;
    }

    const clientMessageId = crypto.randomUUID();
    const path = `${this.conversationId}/${clientMessageId}.${ext}`;
    const entry = {
      client_message_id: clientMessageId,
      conversation_id: this.conversationId,
      sender_id: this.currentUserId,
      content: (caption || '').trim().slice(0, MAX_CONTENT_LENGTH) || MEDIA_PLACEHOLDER,
      reply_to_client_message_id: replyToId || null,
      media_url: path,
      media_type: file.type,
      created_at_local: new Date().toISOString(),
      status: 'queued'
    };

    const preview = URL.createObjectURL(file);
    this.blobUrls.push(preview);
    this.renderMessage({ ...entry, _localPreview: preview }, 'sending');

    const removeRow = () => {
      const row = document.querySelector(`[data-client-id="${cssEscape(clientMessageId)}"]`);
      if (row) row.remove();
    };

    let uploadError = null;
    try {
      const res = await withTimeout(
        supabaseClient.storage.from(MEDIA_BUCKET).upload(path, file, { contentType: file.type, upsert: false }),
        UPLOAD_TIMEOUT_MS
      );
      uploadError = res.error;
    } catch (e) {
      uploadError = e;
    }

    if (uploadError) {
      console.error('Échec upload média:', uploadError);
      if (!this.stopped) { removeRow(); showToast("Échec de l'envoi de l'image. Réessaie."); playSound('failed'); }
      return false;
    }

    // L'upload a réussi : l'intention de l'utilisateur est persistée même si la conversation a changé.
    // Manager arrêté => aucun accès DOM (la ligne appartient à un chat désormais vidé).
    const outbox = this.loadOutbox();
    if (outbox.length >= MAX_OUTBOX) {
      console.error('Outbox pleine après upload : fichier orphelin dans Storage', path);
      if (!this.stopped) { removeRow(); showToast("File d'attente pleine, image non envoyée."); playSound('failed'); }
      return false;
    }
    outbox.push(entry);
    this.saveOutbox(outbox);

    if (this.stopped) return true;
    await this.flushOutbox();
    return true;
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
      if (content) { content.textContent = 'Message supprimé'; content.hidden = false; }
      const media = row.querySelector('.chat-message-media');
      if (media) media.remove();
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

  // ---------- FLUSH OUTBOX (non réentrant, sans écrasement) ----------

  async flushOutbox() {
    if (this.state !== 'READY' || this.stopped) return;
    if (this.flushing) { this.flushQueued = true; return; }
    this.flushing = true;
    try {
      do {
        this.flushQueued = false;
        await this._flushOnce();
      } while (this.flushQueued && this.state === 'READY' && !this.stopped);
    } finally {
      this.flushing = false;
    }
  }

  async _flushOnce() {
    const snapshot = this.loadOutbox();
    const removeIds = new Set();

    for (const entry of snapshot) {
      if (this.stopped || this.state !== 'READY') break;
      const id = entry.client_message_id;

      if (entry.status === 'sent') { removeIds.add(id); continue; }

      if (entry.media_url && !isValidMediaPath(entry.media_url, entry.conversation_id)) {
        console.error('Entrée Outbox média invalide, supprimée:', id);
        removeIds.add(id);
        continue;
      }

      this.patchOutboxEntry(id, { status: 'sending' });
      renderMessageStatus(id, 'sending');

      const payload = {
        conversation_id: entry.conversation_id,
        sender_id: entry.sender_id,
        client_message_id: id,
        content: entry.content,
        reply_to_client_message_id: entry.reply_to_client_message_id || null
      };
      if (entry.media_url) {
        payload.media_url = entry.media_url;
        payload.media_type = entry.media_type;
      }

      let insertResult;
      try {
        insertResult = await withTimeout(
          supabaseClient.from('messages').insert(payload),
          SEND_TIMEOUT_MS
        );
      } catch (timeoutErr) {
        // Watchdog : l'état optimiste ne reste jamais bloqué. Le message reste en Outbox ;
        // un prochain flush retentera, le contrôle 23505 protège contre un doublon.
        this.patchOutboxEntry(id, { status: 'failed' });
        renderMessageStatus(id, 'failed');
        playSound('failed');
        continue;
      }

      const { error } = insertResult;

      if (!error) {
        removeIds.add(id);
        renderMessageStatus(id, 'sent');
        playSound('sent');
        continue;
      }

      if (error.code === '23505') {
        const { data: confirmRow } = await supabaseClient
          .from('messages')
          .select('sender_id, conversation_id, client_message_id')
          .eq('client_message_id', id)
          .maybeSingle();

        const matches = confirmRow
          && confirmRow.sender_id === entry.sender_id
          && confirmRow.conversation_id === entry.conversation_id;

        if (matches) {
          removeIds.add(id);
          renderMessageStatus(id, 'sent');
        } else {
          this.patchOutboxEntry(id, { status: 'failed' });
          renderMessageStatus(id, 'failed');
        }
        // Pas de son : simple réconciliation de retry.
        continue;
      }

      console.error('Rejet sécurité messages:', error);
      this.patchOutboxEntry(id, { status: 'failed' });
      renderMessageStatus(id, 'failed');
      playSound('failed');
    }

    // Relecture fraîche : ne retire que les entrées confirmées, conserve celles ajoutées pendant le flush.
    if (removeIds.size > 0) {
      this.saveOutbox(this.loadOutbox().filter(e => !removeIds.has(e.client_message_id)));
    }
  }

  // ---------- RENDU / DÉDUPLICATION (I5, XSS) ----------
  // prepend = true : insertion en tête (pagination), sans scrollIntoView.

  renderMessage(msg, kind, prepend = false) {
    const id = msg.client_message_id;
    if (!id) return;
    const isMine = msg.sender_id === this.currentUserId;
    const container = document.getElementById('chat-messages');
    if (!container) return;

    let row = document.querySelector(`[data-client-id="${cssEscape(id)}"]`);
    if (!row) {
      const wasNearBottom = container.scrollHeight - container.scrollTop - container.clientHeight < 120;

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

      // Image : jamais affichée sans chemin valide ; URL signée résolue de façon asynchrone.
      const hasMedia = !msg.deleted_for_everyone && msg.media_url && MEDIA_EXT[msg.media_type]
        && (msg._localPreview || isValidMediaPath(msg.media_url, this.conversationId));
      if (hasMedia) {
        const img = document.createElement('img');
        img.className = 'chat-message-media';
        img.alt = 'Photo envoyée';
        img.addEventListener('load', () => {
          if (!prepend && (wasNearBottom || isMine)) container.scrollTop = container.scrollHeight;
        });
        img.addEventListener('click', (e) => {
          e.stopPropagation();
          if (img.src) window.open(img.src, '_blank', 'noopener');
        });
        bubble.appendChild(img);
        if (msg._localPreview) {
          img.src = msg._localPreview;
        } else {
          getSignedMediaUrl(msg.media_url).then(url => {
            if (url) img.src = url;
            else { img.remove(); const miss = document.createElement('span'); miss.className = 'chat-message-status'; miss.textContent = 'Image indisponible'; bubble.insertBefore(miss, bubble.firstChild); }
          });
        }
      }

      const content = document.createElement('span');
      content.className = 'chat-message-content';
      content.textContent = msg.deleted_for_everyone ? 'Message supprimé' : msg.content;
      if (msg.deleted_for_everyone) bubble.classList.add('deleted');
      if (hasMedia && msg.content === MEDIA_PLACEHOLDER) content.hidden = true;
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

      if (prepend) {
        container.insertBefore(row, container.firstChild);
      } else {
        container.appendChild(row);
        row.scrollIntoView({ block: 'nearest' });
      }
    } else if (msg.seq) {
      row.dataset.seq = msg.seq;
    }
    this.updateStatusNode(row, kind === 'confirmed' ? 'sent' : kind);
    if (isMine && msg.seq && !prepend) this.refreshOwnMessageReceipts();
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

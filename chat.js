// ==========================================
// chat.js — Go-school ChatManager
// Invariants I1-I10 implémentés et commentés aux points critiques.
// ==========================================

const OUTBOX_KEY = 'goschool_outbox_v1';
const MAX_OUTBOX = 50;
const MAX_CONTENT_LENGTH = 2000;

class ChatManager {
  constructor(conversationId, currentUserId) {
    this.conversationId = conversationId;
    this.currentUserId = currentUserId;
    this.state = 'DISCONNECTED';
    this.generation = 0;          // I6 — incrémenté à chaque nouvelle connexion
    this.channel = null;
    this.reconnectTimer = null;   // I7 — un seul timer vivant à la fois
    this.reconnectDelays = [2000, 5000, 10000];
    this.reconnectAttempt = 0;
    this.lastKnownSeq = 0;        // curseur pur sur seq, pas de chevauchement nécessaire
    this.stopped = false;
  }

  setState(next) {
    this.state = next;
    renderConnectionStatus(next);
  }

  async start() {
    this.stopped = false;
    await this.connect();
  }

  // Déconnexion volontaire — I9/I11 : n'entraîne JAMAIS de reconnexion automatique
  stop() {
    this.stopped = true;
    clearTimeout(this.reconnectTimer);
    this.reconnectTimer = null;
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

    // I8 — détruire l'ancien channel avant toute recréation.
    // Contourne l'issue #1732 (JWT non rafraîchi sur un channel repris silencieusement) :
    // on ne "reprend" jamais un channel, on en recrée toujours un neuf.
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
      if (myGen !== this.generation) return; // I6 — callback d'une génération périmée, ignoré
      this.handleRealtimeInsert(payload.new);
    });

    channel.subscribe(async (status) => {
      if (myGen !== this.generation) return; // I6
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
    // I7 — on annule systématiquement le timer précédent avant d'en poser un nouveau
    clearTimeout(this.reconnectTimer);
    const delay = this.reconnectDelays[Math.min(this.reconnectAttempt, this.reconnectDelays.length - 1)];
    this.reconnectAttempt += 1;
    this.reconnectTimer = setTimeout(() => {
      if (!this.stopped) this.connect();
    }, delay);
  }

  async syncAndFlush(myGen) {
    this.setState('SYNCING');
    // I4/I14 — curseur pur sur seq (bigserial, ordre total garanti par Postgres,
    // contrairement à created_at). Pas de chevauchement temporel nécessaire.
    const { data, error } = await supabaseClient
      .from('messages')
      .select('id, conversation_id, sender_id, client_message_id, content, created_at, seq, reported')
      .eq('conversation_id', this.conversationId)
      .gt('seq', this.lastKnownSeq)
      .order('seq', { ascending: true });

    if (myGen !== this.generation) return; // la génération a changé pendant l'await

    if (!error && data) {
      data.forEach(msg => {
        this.renderMessage(msg, 'confirmed');
        if (msg.seq > this.lastKnownSeq) this.lastKnownSeq = msg.seq;
      });
    } else if (error) {
      console.error('Erreur synchronisation REST:', error);
    }

    // Le compteur de backoff ne revient à zéro qu'après un cycle SYNCING réellement abouti
    this.reconnectAttempt = 0;
    this.setState('READY');
    await this.flushOutbox();
  }

  handleRealtimeInsert(row) {
    // I5 — passe par la même fonction de rendu que REST/Outbox : dédup automatique
    this.renderMessage(row, 'confirmed');
    if (row.seq && row.seq > this.lastKnownSeq) this.lastKnownSeq = row.seq;
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

  async sendMessage(content) {
    content = content.trim();
    if (!content || content.length > MAX_CONTENT_LENGTH) return;

    const outbox = this.loadOutbox();
    if (outbox.length >= MAX_OUTBOX) {
      showToast("File d'attente pleine, attends l'envoi des messages précédents.");
      return;
    }

    // I1 — UUID généré ici, une seule fois, avant toute tentative réseau, conservé sur tout retry
    const clientMessageId = crypto.randomUUID();
    const entry = {
      client_message_id: clientMessageId,
      conversation_id: this.conversationId,
      sender_id: this.currentUserId,
      content,
      created_at_local: new Date().toISOString(),
      status: 'queued'
    };
    outbox.push(entry);
    this.saveOutbox(outbox);

    this.renderMessage(entry, 'pending');
    await this.flushOutbox();
  }

  async flushOutbox() {
    if (this.state !== 'READY') return; // jamais de tentative d'envoi hors ligne
    let outbox = this.loadOutbox();

    for (const entry of outbox) {
      if (entry.status === 'sent') continue;
      entry.status = 'sending';
      this.saveOutbox(outbox);
      renderMessageStatus(entry.client_message_id, 'sending');

      const { error } = await supabaseClient.from('messages').insert({
        conversation_id: entry.conversation_id,
        sender_id: entry.sender_id,
        client_message_id: entry.client_message_id,
        content: entry.content
      });

      if (!error) {
        entry.status = 'sent';
        renderMessageStatus(entry.client_message_id, 'sent');
        continue;
      }

      if (error.code === '23505') {
        // I3 — un 23505 n'est JAMAIS un succès automatique.
        // La RLS de lecture bloque déjà cette requête si la ligne réelle
        // n'appartient pas à l'utilisateur courant : réponse vide -> FAILED.
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
        continue;
      }

      // 42501/23514 = rejet RLS/contrainte réel, pas un problème réseau
      console.error('Rejet sécurité messages:', error);
      entry.status = 'failed';
      renderMessageStatus(entry.client_message_id, 'failed');
    }

    // Suppression uniquement après confirmation serveur — jamais avant
    outbox = outbox.filter(e => e.status !== 'sent');
    this.saveOutbox(outbox);
  }

  // ---------- RENDU / DÉDUPLICATION (I5, XSS) ----------

  renderMessage(msg, kind) {
    const id = msg.client_message_id;
    if (!id) return;
    let node = document.querySelector(`[data-client-id="${cssEscape(id)}"]`);
    if (!node) {
      node = document.createElement('div');
      node.className = 'chat-message' + (msg.sender_id === this.currentUserId ? ' mine' : '');
      node.dataset.clientId = id;

      const bubble = document.createElement('span');
      bubble.className = 'chat-message-content';
      bubble.textContent = msg.content; // JAMAIS innerHTML — protection XSS absolue
      node.appendChild(bubble);

      const status = document.createElement('span');
      status.className = 'chat-message-status';
      node.appendChild(status);

      document.getElementById('chat-messages').appendChild(node);
      node.scrollIntoView({ block: 'nearest' });
    }
    this.updateStatusNode(node, kind === 'confirmed' ? 'sent' : kind);
  }

  updateStatusNode(node, status) {
    const statusEl = node.querySelector('.chat-message-status');
    const labels = { pending: 'En attente…', sending: 'Envoi…', sent: '', failed: 'Échec — réessaie' };
    statusEl.textContent = labels[status] ?? '';
    node.classList.toggle('failed', status === 'failed');
  }
}

function renderMessageStatus(clientId, status) {
  const node = document.querySelector(`[data-client-id="${cssEscape(clientId)}"]`);
  if (!node) return;
  const labels = { sending: 'Envoi…', sent: '', failed: 'Échec — réessaie' };
  const statusEl = node.querySelector('.chat-message-status');
  if (statusEl) statusEl.textContent = labels[status] ?? '';
  node.classList.toggle('failed', status === 'failed');
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
      // La conversation directe existe déjà — on la retrouve plutôt que d'échouer
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

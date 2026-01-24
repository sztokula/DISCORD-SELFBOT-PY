import queue
import threading
import time
from collections import defaultdict, deque


class AutoReplyService:
    def __init__(self, db_manager, worker, responder, log_callback=None):
        self.db = db_manager
        self.worker = worker
        self.responder = responder
        self.log = log_callback
        self._queue = queue.Queue()
        self._stop = threading.Event()
        self._self_user_ids = {}
        self._recent_by_token = defaultdict(lambda: deque(maxlen=200))
        self._last_message_id_by_channel = {}
        self._replied_channels = {}
        self._replied_channel_ttl_seconds = 86400.0
        self._token_meta = {}
        self._token_meta_last_refresh = 0.0
        self._token_meta_refresh_seconds = 60.0
        self._thread = threading.Thread(target=self._run, daemon=True)
        self._thread.start()

    def stop(self):
        self._stop.set()
        try:
            self._queue.put_nowait(None)
        except Exception:
            pass

    def _log(self, message):
        if self.log:
            self.log(message)

    def _auto_reply_enabled(self):
        value = self.db.get_setting("auto_reply_enabled", None)
        if value in (None, ""):
            return False
        if isinstance(value, str):
            return value.strip().lower() in {"1", "true", "yes", "on"}
        return bool(value)

    def _reply_once_per_conversation(self):
        value = self.db.get_setting("auto_reply_once_per_conversation", None)
        if value in (None, ""):
            return False
        if isinstance(value, str):
            return value.strip().lower() in {"1", "true", "yes", "on"}
        return bool(value)

    def _refresh_token_meta(self):
        mapping = {}
        for acc in self.db.get_active_accounts("discord"):
            acc_id = acc[0]
            token = acc[2]
            proxy = acc[3] if len(acc) > 3 else None
            if token:
                mapping[token] = {"account_id": acc_id, "proxy": proxy}
        self._token_meta = mapping
        self._token_meta_last_refresh = time.monotonic()
        self._log(f"[Debug] Token metadata refreshed: {len(mapping)} account(s).")

    def _get_token_meta(self, token):
        now = time.monotonic()
        if now - self._token_meta_last_refresh > self._token_meta_refresh_seconds:
            self._refresh_token_meta()
        meta = self._token_meta.get(token)
        if meta:
            return meta
        self._refresh_token_meta()
        return self._token_meta.get(token)

    def handle_event(self, token, payload):
        if not token or not isinstance(payload, dict):
            return
        event_type = payload.get("t")
        data = payload.get("d") or {}
        if event_type == "READY":
            user = data.get("user") or {}
            user_id = user.get("id")
            if user_id:
                self._self_user_ids[token] = user_id
                self._log(f"[Debug] Gateway READY received: self_user_id={user_id}.")
            return
        if event_type != "MESSAGE_CREATE":
            return
        if not self._auto_reply_enabled():
            self._log("[Debug] Auto-reply disabled; MESSAGE_CREATE ignored.")
            return
        if not self.responder.is_enabled():
            self._log("[Debug] AI responder disabled; MESSAGE_CREATE ignored.")
            return
        if data.get("guild_id"):
            self._log("[Debug] Guild message ignored for auto-reply.")
            return
        author = data.get("author") or {}
        author_id = author.get("id")
        if not author_id:
            self._log("[Debug] MESSAGE_CREATE ignored: missing author_id.")
            return
        if author.get("bot"):
            self._log("[Debug] MESSAGE_CREATE ignored: author is bot.")
            return
        if author_id and author_id == self._self_user_ids.get(token):
            self._log("[Debug] MESSAGE_CREATE ignored: self message.")
            return
        message_id = data.get("id")
        if not message_id:
            self._log("[Debug] MESSAGE_CREATE ignored: missing message_id.")
            return
        channel_id = data.get("channel_id")
        if not channel_id:
            self._log("[Debug] MESSAGE_CREATE ignored: missing channel_id.")
            return
        if self._reply_once_per_conversation():
            self._prune_replied_channels()
            if channel_id in self._replied_channels:
                self._log("[Debug] MESSAGE_CREATE ignored: already replied in this channel.")
                return
        last_id = self._last_message_id_by_channel.get(channel_id)
        if last_id:
            try:
                if int(message_id) <= int(last_id):
                    self._log("[Debug] MESSAGE_CREATE ignored: older or duplicate message_id.")
                    return
            except Exception:
                if message_id == last_id:
                    self._log("[Debug] MESSAGE_CREATE ignored: duplicate message_id.")
                    return
        self._last_message_id_by_channel[channel_id] = message_id
        recent = self._recent_by_token[token]
        if message_id in recent:
            self._log("[Debug] MESSAGE_CREATE ignored: recently processed message_id.")
            return
        recent.append(message_id)
        content = (data.get("content") or "").strip()
        if not channel_id or not content:
            self._log("[Debug] MESSAGE_CREATE ignored: empty content.")
            return
        if self._reply_once_per_conversation():
            self._replied_channels[channel_id] = time.monotonic()

    def _prune_replied_channels(self):
        if not self._replied_channels:
            return
        cutoff = time.monotonic() - self._replied_channel_ttl_seconds
        stale = [cid for cid, ts in self._replied_channels.items() if ts < cutoff]
        for cid in stale:
            self._replied_channels.pop(cid, None)
        if stale:
            self._log(f"[Debug] Pruned replied channels: removed {len(stale)}.")
        author_name = author.get("username") or author.get("global_name")
        try:
            self._queue.put_nowait(
                {
                    "token": token,
                    "channel_id": channel_id,
                    "content": content,
                    "author_name": author_name,
                }
            )
        except Exception:
            return

    def _run(self):
        while not self._stop.is_set():
            try:
                item = self._queue.get(timeout=0.5)
            except queue.Empty:
                continue
            if not item:
                continue
            token = item.get("token")
            channel_id = item.get("channel_id")
            content = item.get("content")
            author_name = item.get("author_name")
            if not token or not channel_id or not content:
                continue
            context = f"ch:{str(channel_id)[-6:]}"
            reply = self.responder.generate_reply(content, author_name=author_name, token=token)
            if not reply:
                self._log(f"[AI] Reply generation failed ({context}).")
                continue
            meta = self._get_token_meta(token) or {}
            account_id = meta.get("account_id")
            proxy = meta.get("proxy")
            ok, info = self.worker.send_channel_message(
                account_id,
                token,
                channel_id,
                reply,
                proxy=proxy,
            )
            if not ok:
                self._log(f"[AI] Auto-reply failed ({context}): {info}")
                self._log(f"[Error] Auto-reply send failed ({context}): {info}")
            else:
                self._log(f"[Info] Auto-reply sent ({context}).")

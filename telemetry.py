import base64
import json
import threading
import time

import httpx

from proxy_utils import httpx_client
from super_properties import set_super_properties_header
from client_identity import USER_AGENT


class TelemetryClient:
    def __init__(self, db_manager, log_callback=None, min_interval_seconds=300):
        self.db = db_manager
        self.log = log_callback
        self.min_interval_seconds = max(5, int(min_interval_seconds))
        self._last_sent = {}
        self._lock = threading.Lock()

    def _telemetry_killed(self):
        raw = ""
        try:
            raw = self.db.get_setting("telemetry_kill_switch", "")
        except Exception:
            raw = ""
        if raw in (None, ""):
            return True
        if isinstance(raw, str):
            return raw.strip().lower() in {"1", "true", "yes", "on"}
        return bool(raw)

    def _should_send(self, token):
        if not token:
            return False
        now = time.monotonic()
        with self._lock:
            last = self._last_sent.get(token)
            if last and (now - last) < self.min_interval_seconds:
                return False
            self._last_sent[token] = now
        return True

    def _build_payload(self, event_name, properties=None):
        ts_ms = int(time.time() * 1000)
        event = {
            "type": event_name,
            "timestamp": ts_ms,
            "properties": properties or {},
        }
        return {
            "events": [event],
            "client_send_timestamp": ts_ms,
        }

    def _encode_payload(self, payload):
        raw = json.dumps(payload, separators=(",", ":"), ensure_ascii=True).encode("utf-8")
        return base64.b64encode(raw).decode("ascii")

    def send_science(self, token, user_agent, event_name, properties=None, proxy=None):
        if self._telemetry_killed():
            if self.log:
                self.log("[Telemetry] Kill switch enabled; request blocked.")
            return False
        if not self._should_send(token):
            return False
        payload = self._build_payload(event_name, properties=properties)
        encoded = self._encode_payload(payload)
        body = {"payload": encoded, "encoding": "base64"}
        headers = {
            "Content-Type": "application/json",
            "User-Agent": user_agent or USER_AGENT,
        }
        if token:
            headers["Authorization"] = token
        set_super_properties_header(headers, self.db, user_agent=user_agent)
        try:
            with httpx_client(
                proxy,
                headers=headers,
                timeout=httpx.Timeout(10.0),
                cookie_db=self.db,
                cookie_token=token,
            ) as client:
                resp = client.post("https://discord.com/api/v9/science", json=body)
            if resp.status_code not in (200, 204):
                if self.log:
                    self.log(f"[Telemetry] science error: {resp.status_code}")
                return False
            return True
        except Exception as exc:
            if self.log:
                self.log(f"[Telemetry] science exception: {exc}")
            return False

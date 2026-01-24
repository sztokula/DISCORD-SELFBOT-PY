import asyncio
import json
import time
import threading
import random
from typing import Callable, Dict, Optional

try:
    import websockets
except Exception as exc:  # pragma: no cover - optional dependency at runtime
    websockets = None
    _WEBSOCKETS_IMPORT_ERROR = exc
else:
    _WEBSOCKETS_IMPORT_ERROR = None

GATEWAY_URL = "wss://gateway.discord.gg/?v=9&encoding=json"
from proxy_utils import normalize_proxy


class GatewayClient:
    def __init__(
        self,
        token: str,
        log: Optional[Callable[[str], None]] = None,
        heartbeat_interval_seconds: float = 40.0,
        properties: Optional[Dict[str, str]] = None,
        proxy: Optional[str] = None,
        intents: int = 4096,
        on_connect: Optional[Callable[[str], None]] = None,
        on_disconnect: Optional[Callable[[str], None]] = None,
        on_event: Optional[Callable[[str, Dict], None]] = None,
    ):
        self.token = token
        self.log = log
        self.heartbeat_interval_seconds = heartbeat_interval_seconds
        self.properties = properties or {
            "os": "Windows",
            "browser": "Chrome",
            "device": "",
        }
        self.proxy = proxy
        self.intents = intents
        self.on_connect = on_connect
        self.on_disconnect = on_disconnect
        self.on_event = on_event
        self._stop = asyncio.Event()
        self._last_sequence = None

    def stop(self):
        self._stop.set()

    async def run(self):
        if websockets is None:
            raise RuntimeError(
                f"Missing dependency 'websockets': {_WEBSOCKETS_IMPORT_ERROR}"
            )
        while not self._stop.is_set():
            try:
                async with websockets.connect(
                    GATEWAY_URL,
                    ping_interval=None,
                    ping_timeout=None,
                    max_queue=32,
                    proxy=self.proxy,
                ) as ws:
                    await self._handle_connection(ws)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                self._log(f"[Gateway] reconnecting after error: {exc}")
                await asyncio.sleep(5)

    async def _handle_connection(self, ws):
        if self.on_connect:
            try:
                self.on_connect(self.token)
            except Exception:
                pass
        hello_payload = await ws.recv()
        interval = self._extract_heartbeat_interval(hello_payload)
        await ws.send(json.dumps(self._identify_payload()))
        self._log("[Gateway] IDENTIFY sent")
        heartbeat_task = asyncio.create_task(self._heartbeat_loop(ws, interval))
        try:
            await self._recv_loop(ws)
        finally:
            heartbeat_task.cancel()
            try:
                await heartbeat_task
            except Exception:
                pass
            if self.on_disconnect:
                try:
                    self.on_disconnect(self.token)
                except Exception:
                    pass

    def _extract_heartbeat_interval(self, payload):
        interval = self.heartbeat_interval_seconds
        try:
            data = json.loads(payload)
            if data.get("op") == 10:
                raw = data.get("d", {}).get("heartbeat_interval")
                if isinstance(raw, (int, float)) and raw > 0:
                    interval = max(1.0, raw / 1000.0)
        except Exception:
            pass
        return interval

    def _identify_payload(self):
        return {
            "op": 2,
            "d": {
                "token": self.token,
                "properties": self.properties,
                "presence": {
                    "status": "online",
                    "since": 0,
                    "activities": [],
                    "afk": False,
                },
                "compress": False,
                "intents": int(self.intents) if self.intents is not None else 0,
            },
        }

    async def _recv_loop(self, ws):
        while not self._stop.is_set():
            try:
                message = await ws.recv()
            except asyncio.CancelledError:
                raise
            except Exception:
                break

            payload = self._safe_json(message)
            if not payload:
                continue
            if self.on_event:
                try:
                    self.on_event(self.token, payload)
                except Exception:
                    pass
            if "s" in payload:
                self._last_sequence = payload["s"]
            op = payload.get("op")
            if op == 1:  # HEARTBEAT request
                await self._send_heartbeat(ws)
            elif op == 7:  # RECONNECT
                break
            elif op == 9:  # INVALID_SESSION
                break
            elif op == 11:  # HEARTBEAT ACK
                continue

    async def _heartbeat_loop(self, ws, interval):
        jitter = random.uniform(0.0, max(0.1, interval))
        await asyncio.sleep(jitter)
        while not self._stop.is_set():
            await self._send_heartbeat(ws)
            await asyncio.sleep(interval)

    async def _send_heartbeat(self, ws):
        try:
            await ws.send(json.dumps({"op": 1, "d": self._last_sequence}))
        except Exception:
            return

    def _safe_json(self, message):
        try:
            return json.loads(message)
        except Exception:
            return None

    def _log(self, message):
        if self.log:
            self.log(message)


class GatewayManager:
    def __init__(self, db_manager, log: Optional[Callable[[str], None]] = None, on_event=None):
        self.db = db_manager
        self.log = log
        self.on_event = on_event
        self._clients = []
        self._tasks = []
        self._stop = asyncio.Event()
        self._active_tokens = set()
        self._active_lock = threading.Lock()

    def stop(self):
        for client in self._clients:
            client.stop()
        self._stop.set()

    async def run(self):
        accounts = self._load_active_accounts()
        if not accounts:
            self._log("[Gateway] No active tokens found.")
            return
        for token, proxy in accounts:
            client = GatewayClient(
                token,
                log=self.log,
                proxy=proxy,
                on_connect=self._mark_connected,
                on_disconnect=self._mark_disconnected,
                on_event=self.on_event,
            )
            self._clients.append(client)
            self._tasks.append(asyncio.create_task(client.run()))
        await self._stop.wait()
        for task in self._tasks:
            task.cancel()

    def _load_active_accounts(self):
        accounts = self.db.get_active_accounts("discord")
        results = []
        for acc in accounts:
            token = acc[2]
            proxy = normalize_proxy(acc[3]) if len(acc) > 3 else ""
            proxy = proxy or None
            if token:
                results.append((token, proxy))
        return results

    def _log(self, message):
        if self.log:
            self.log(message)

    def _mark_connected(self, token):
        if not token:
            return
        with self._active_lock:
            self._active_tokens.add(token)

    def _mark_disconnected(self, token):
        if not token:
            return
        with self._active_lock:
            if token in self._active_tokens:
                self._active_tokens.remove(token)

    def is_connected(self, token):
        if not token:
            return False
        with self._active_lock:
            return token in self._active_tokens


async def run_gateway_for_active_tokens(db_manager, log=None):
    manager = GatewayManager(db_manager, log=log)
    await manager.run()

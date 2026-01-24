import asyncio
import ctypes
import json
import time
import threading
import random
import sys
from typing import Callable, Dict, Optional

GATEWAY_URL = "wss://gateway.discord.gg/?v=9&encoding=json"
from proxy_utils import normalize_proxy, ws_connect
from super_properties import build_gateway_properties, get_token_user_agent
from database import DatabaseManager


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
        user_agent: Optional[str] = None,
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
        self._heartbeat_stop = threading.Event()
        self._heartbeat_thread = None
        self.user_agent = user_agent

    def stop(self):
        self._stop.set()

    async def run(self):
        reconnects = 0
        while not self._stop.is_set():
            try:
                async with ws_connect(
                    GATEWAY_URL,
                    proxy=self.proxy,
                    headers={"User-Agent": self.user_agent},
                    timeout=30.0,
                ) as ws:
                    reconnects = 0
                    await self._handle_connection(ws)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                reconnects += 1
                self._log(f"[Gateway] reconnect attempt {reconnects} after error: {exc}")
                self._log(f"[Gateway] reconnecting after error: {exc}")
                self._log(f"[Error] Gateway connection failed: {exc}")
                await asyncio.sleep(5)

    async def _handle_connection(self, ws):
        if self.on_connect:
            try:
                self.on_connect(self.token)
            except Exception:
                pass
        hello_payload = self._normalize_ws_message(await ws.recv())
        interval = self._extract_heartbeat_interval(hello_payload)
        await ws.send(json.dumps(self._identify_payload()))
        self._log("[Gateway] IDENTIFY sent")
        loop = asyncio.get_running_loop()
        self._start_heartbeat_thread(ws, interval, loop)
        try:
            await self._recv_loop(ws)
        finally:
            self._stop_heartbeat_thread()
            try:
                await asyncio.sleep(0)
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
                message = self._normalize_ws_message(await ws.recv())
            except asyncio.CancelledError:
                raise
            except Exception:
                break

            payload = self._safe_json(message)
            if not payload:
                continue
            event_type = payload.get("t")
            if event_type == "READY":
                self._log("[Gateway] READY received")
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
                self._log("[Gateway] RECONNECT requested")
                break
            elif op == 9:  # INVALID_SESSION
                self._log("[Gateway] INVALID_SESSION received")
                break
            elif op == 11:  # HEARTBEAT ACK
                continue

    def _start_heartbeat_thread(self, ws, interval, loop):
        self._heartbeat_stop.set()
        if self._heartbeat_thread and self._heartbeat_thread.is_alive():
            return
        self._heartbeat_stop.clear()
        self._heartbeat_thread = threading.Thread(
            target=self._heartbeat_loop_thread,
            args=(ws, interval, loop),
            daemon=True,
        )
        self._heartbeat_thread.start()

    def _stop_heartbeat_thread(self):
        self._heartbeat_stop.set()
        if self._heartbeat_thread and self._heartbeat_thread.is_alive():
            try:
                self._heartbeat_thread.join(timeout=2.0)
            except Exception:
                pass

    def _heartbeat_loop_thread(self, ws, interval, loop):
        self._bump_thread_priority()
        jitter = random.uniform(0.0, max(0.1, interval))
        time.sleep(jitter)
        while not self._stop.is_set() and not self._heartbeat_stop.is_set():
            try:
                asyncio.run_coroutine_threadsafe(self._send_heartbeat(ws), loop)
            except Exception:
                return
            time.sleep(interval)

    async def _send_heartbeat(self, ws):
        try:
            await ws.send(json.dumps({"op": 1, "d": self._last_sequence}))
        except Exception:
            return

    def _bump_thread_priority(self):
        if sys.platform != "win32":
            return
        try:
            THREAD_PRIORITY_HIGHEST = 2
            handle = ctypes.windll.kernel32.GetCurrentThread()
            ctypes.windll.kernel32.SetThreadPriority(handle, THREAD_PRIORITY_HIGHEST)
        except Exception:
            return

    def _safe_json(self, message):
        if message is None:
            return None
        try:
            return json.loads(message)
        except Exception:
            return None

    def _normalize_ws_message(self, message):
        if isinstance(message, tuple) and len(message) == 2:
            message = message[0]
        if isinstance(message, (bytes, bytearray, memoryview)):
            try:
                message = bytes(message).decode("utf-8", errors="ignore")
            except Exception:
                return None
        return message

    def _log(self, message):
        if self.log:
            self.log(message)


class GatewayManager:
    def __init__(
        self,
        db_manager,
        log: Optional[Callable[[str], None]] = None,
        on_event=None,
        shared_status: Optional[Dict[str, int]] = None,
    ):
        self.db = db_manager
        self.log = log
        self.on_event = on_event
        self.shared_status = shared_status
        self._clients = []
        self._tasks = []
        self._stop = asyncio.Event()
        self._active_tokens = set()
        self._active_lock = threading.Lock()
        self._last_connected = {}
        self._last_disconnected = {}
        self._last_reconnected = {}

    def stop(self):
        for client in self._clients:
            client.stop()
        self._stop.set()

    async def run(self):
        accounts = self._load_active_accounts()
        if not accounts:
            self._log("[Gateway] No active tokens found.")
            return
        self._log(f"[Info] Gateway starting for {len(accounts)} token(s).")
        for token, proxy in accounts:
            user_agent = get_token_user_agent(self.db, token, proxy=proxy)
            properties = build_gateway_properties(self.db, token=token, proxy=proxy, user_agent=user_agent)
            client = GatewayClient(
                token,
                log=self.log,
                proxy=proxy,
                properties=properties,
                user_agent=user_agent,
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
        require_proxy = self._is_proxy_required()
        if require_proxy:
            try:
                overview = self.db.get_accounts_overview()
            except Exception:
                overview = []
            for acc_id, status, proxy, *_rest in overview:
                status_value = (status or "").strip().casefold()
                if status_value != "unverified":
                    continue
                normalized = normalize_proxy(proxy) if proxy else ""
                if not normalized:
                    continue
                if normalized != proxy:
                    try:
                        self.db.update_account_proxy(acc_id, normalized)
                    except Exception:
                        pass
                try:
                    self.db.update_account_status(acc_id, "Active")
                    self._log(f"[Info] Account {acc_id} restored to Active (proxy set).")
                except Exception:
                    self._log(f"[Error] Failed to restore account {acc_id} to Active.")

        accounts = self.db.get_active_accounts("discord")
        results = []
        for acc in accounts:
            acc_id = acc[0]
            token = acc[2]
            proxy = normalize_proxy(acc[3]) if len(acc) > 3 else ""
            proxy = proxy or None
            if require_proxy and not proxy:
                if token:
                    suffix = str(token)[-6:]
                else:
                    suffix = "unknown"
                self._log(f"[Warn] Gateway skipped token without proxy (token=...{suffix}).")
                if acc_id is not None:
                    try:
                        self.db.update_account_status(acc_id, "Unverified")
                        self._log(f"[Info] Account {acc_id} marked Unverified (missing proxy).")
                    except Exception:
                        self._log(f"[Error] Failed to mark account {acc_id} Unverified.")
                continue
            if token:
                results.append((token, proxy))
        return results

    def _is_proxy_required(self):
        try:
            value = self.db.get_setting("require_proxy", None)
        except Exception:
            return False
        if value in (None, ""):
            return False
        if isinstance(value, str):
            return value.strip().lower() in {"1", "true", "yes", "on"}
        return bool(value)

    def _log(self, message):
        if self.log:
            self.log(message)

    def _mark_connected(self, token):
        if not token:
            return
        now = time.monotonic()
        with self._active_lock:
            self._active_tokens.add(token)
            last_disc = self._last_disconnected.get(token)
            if last_disc is not None and (now - last_disc) < 600:
                self._last_reconnected[token] = now
            self._last_connected[token] = now
        if self.shared_status is not None:
            try:
                self.shared_status[token] = 1
            except Exception:
                pass
        suffix = str(token)[-6:]
        self._log(f"[Info] Gateway connected (token=...{suffix}).")

    def _mark_disconnected(self, token):
        if not token:
            return
        now = time.monotonic()
        with self._active_lock:
            if token in self._active_tokens:
                self._active_tokens.remove(token)
            self._last_disconnected[token] = now
        if self.shared_status is not None:
            try:
                self.shared_status.pop(token, None)
            except Exception:
                pass
        suffix = str(token)[-6:]
        self._log(f"[Warn] Gateway disconnected (token=...{suffix}).")

    def is_connected(self, token):
        if not token:
            return False
        with self._active_lock:
            return token in self._active_tokens

    def was_recently_reconnected(self, token, within_seconds=120):
        if not token:
            return False
        try:
            window = float(within_seconds)
        except (TypeError, ValueError):
            window = 120.0
        with self._active_lock:
            ts = self._last_reconnected.get(token)
        if ts is None:
            return False
        return (time.monotonic() - ts) <= max(0.0, window)


async def run_gateway_for_active_tokens(db_manager, log=None):
    manager = GatewayManager(db_manager, log=log)
    await manager.run()


def run_gateway_process(db_name, log_queue=None, event_queue=None, shared_status=None, stop_event=None):
    def _log(msg):
        if log_queue:
            try:
                log_queue.put(msg)
            except Exception:
                pass

    def _on_event(token, payload):
        if event_queue:
            try:
                event_queue.put((token, payload))
            except Exception:
                pass

    db = DatabaseManager(db_name=db_name, log_callback=None)
    manager = GatewayManager(
        db,
        log=_log,
        on_event=_on_event if event_queue else None,
        shared_status=shared_status,
    )

    if stop_event:
        def _stopper():
            stop_event.wait()
            manager.stop()
        threading.Thread(target=_stopper, daemon=True).start()

    asyncio.run(manager.run())

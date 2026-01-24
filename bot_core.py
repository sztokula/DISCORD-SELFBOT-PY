import multiprocessing
import threading
import time

from auto_reply import AutoReplyService
from build_number_updater import BuildNumberUpdater
from captcha_solver import CaptchaSolver
from discord_worker import DiscordWorker
from gateway import run_gateway_process
from openai_responder import OpenAIResponder
from profile_updater import ProfileUpdater
from scraper import DiscordScraper
from status_changer import StatusChanger
from telemetry import TelemetryClient
from token_manager import TokenManager
from joiner import DiscordJoiner


class GatewayProcessProxy:
    def __init__(self, shared_status, stop_event, process):
        self._shared_status = shared_status
        self._stop_event = stop_event
        self._process = process

    def is_connected(self, token):
        if not token:
            return False
        try:
            return bool(self._shared_status.get(token))
        except Exception:
            return False

    def stop(self):
        if self._stop_event:
            try:
                self._stop_event.set()
            except Exception:
                pass
        proc = self._process
        if not proc:
            return
        try:
            if proc.is_alive():
                proc.join(timeout=2.0)
        except Exception:
            pass
        try:
            if proc.is_alive():
                proc.terminate()
        except Exception:
            pass


class BotCore:
    def __init__(self, db, log_callback=None, metrics=None, critical_callback=None):
        self.db = db
        self.log = log_callback
        self.metrics = metrics

        self.captcha_solver = CaptchaSolver(self.db, self.log)
        self.telemetry = TelemetryClient(self.db, self.log)

        self.gateway_mp_manager = multiprocessing.Manager()
        self.gateway_shared_status = self.gateway_mp_manager.dict()
        self.gateway_log_queue = multiprocessing.Queue()
        self.gateway_event_queue = multiprocessing.Queue()
        self.gateway_stop_event = multiprocessing.Event()
        self.gateway_process = multiprocessing.Process(
            target=run_gateway_process,
            args=(
                self.db.db_name,
                self.gateway_log_queue,
                self.gateway_event_queue,
                self.gateway_shared_status,
                self.gateway_stop_event,
            ),
            daemon=True,
        )
        self.gateway_process.start()
        self.gateway_manager = GatewayProcessProxy(
            self.gateway_shared_status,
            self.gateway_stop_event,
            self.gateway_process,
        )

        self.worker = DiscordWorker(
            self.db,
            self.log,
            self.metrics,
            self.captcha_solver,
            self.telemetry,
            self.gateway_manager,
        )
        self.openai_responder = OpenAIResponder(self.db, self.log)
        self.auto_reply_service = AutoReplyService(
            self.db,
            self.worker,
            self.openai_responder,
            self.log,
        )
        self.scraper = DiscordScraper(self.db, self.log, self.metrics, self.telemetry)
        self.status_changer = StatusChanger(self.db, self.log, self.metrics, self.telemetry)
        self.joiner = DiscordJoiner(self.db, self.log, self.captcha_solver, self.metrics, self.telemetry)
        self.token_manager = TokenManager(self.db, self.log, self.metrics, self.telemetry)
        self.profile_updater = ProfileUpdater(self.db, self.log, self.metrics, self.telemetry)
        self.build_number_updater = BuildNumberUpdater(
            self.db,
            self.log,
            critical_callback=critical_callback,
        )
        self._build_number_thread = threading.Thread(
            target=self.build_number_updater.run_forever,
            daemon=True,
        )
        self._build_number_thread.start()

        self._mission_thread = None
        self._mission_watchdog_thread = None
        self._mission_watchdog_stop = threading.Event()
        self._mission_restart_lock = threading.Lock()
        self._mission_restart_in_progress = False
        self._last_mission_params = None

    def _log(self, message):
        if self.log:
            self.log(message)

    def _launch_mission_with_params(self, params):
        if not params:
            return
        templates = params.get("templates") or []
        dm_delay_min = int(params.get("dm_delay_min", 0))
        dm_delay_max = int(params.get("dm_delay_max", 0))
        friend_delay_min = int(params.get("friend_delay_min", 0))
        friend_delay_max = int(params.get("friend_delay_max", 0))
        account_min_interval = int(params.get("account_min_interval", 0))
        target_min_interval = int(params.get("target_min_interval", 0))
        use_friend_req = bool(params.get("use_friend_req"))
        dry_run = bool(params.get("dry_run"))
        allowed_ids = params.get("allowed_ids")

        def _mission_worker():
            self.worker.reset_heartbeat()
            self.worker.run_mission(
                templates,
                dm_delay_min,
                dm_delay_max,
                use_friend_req,
                friend_delay_min,
                friend_delay_max,
                account_min_interval,
                target_min_interval,
                dry_run,
                allowed_ids,
            )

        thread = threading.Thread(target=_mission_worker, daemon=True)
        self._mission_thread = thread
        thread.start()

    def start_mission(self, params):
        self._last_mission_params = params
        self._launch_mission_with_params(params)
        self._ensure_mission_watchdog()
        self._log("[Info] Mission started.")

    def _mission_watchdog_loop(self):
        while not self._mission_watchdog_stop.is_set():
            check_interval = max(5.0, float(self.db.get_setting("worker_heartbeat_check_seconds", 30.0)))
            self._mission_watchdog_stop.wait(check_interval)
            if self._mission_watchdog_stop.is_set():
                break
            if not getattr(self.worker, "is_running", False):
                continue
            max_idle = float(self.db.get_setting("worker_max_idle_seconds", 900.0))
            if max_idle <= 0:
                continue
            params = self._last_mission_params or {}
            if params.get("dry_run"):
                continue
            try:
                idle = self.worker.get_last_action_age()
            except Exception:
                idle = 0.0
            if idle < max_idle:
                continue
            with self._mission_restart_lock:
                if self._mission_restart_in_progress:
                    continue
                self._mission_restart_in_progress = True
            try:
                self._log(f"[Watchdog] Worker idle {int(idle)}s. Restarting mission...")
                self.worker.stop()
                time.sleep(1.0)
                try:
                    raw = self.db.get_setting("module_dm", None)
                except Exception:
                    raw = None
                if isinstance(raw, str):
                    enabled = raw.strip().lower() in {"1", "true", "yes", "on"}
                else:
                    enabled = bool(raw) if raw is not None else True
                if not enabled:
                    self._log("[Watchdog] DM module disabled; restart skipped.")
                    continue
                if not self._last_mission_params:
                    self._log("[Watchdog] No stored mission params; restart skipped.")
                    continue
                self._launch_mission_with_params(self._last_mission_params)
            finally:
                with self._mission_restart_lock:
                    self._mission_restart_in_progress = False

    def _ensure_mission_watchdog(self):
        if self._mission_watchdog_thread and self._mission_watchdog_thread.is_alive():
            return
        self._mission_watchdog_stop.clear()
        self._mission_watchdog_thread = threading.Thread(
            target=self._mission_watchdog_loop,
            daemon=True,
        )
        self._mission_watchdog_thread.start()

    def stop_mission(self):
        self._mission_watchdog_stop.set()
        self.worker.stop()

    def stop_all(self):
        self.stop_mission()
        self.scraper.stop()
        self.status_changer.stop()
        self.joiner.stop()
        if self.gateway_manager:
            try:
                self.gateway_manager.stop()
            except Exception:
                pass

    def shutdown(self):
        self.stop_all()
        if self.gateway_mp_manager:
            try:
                self.gateway_mp_manager.shutdown()
            except Exception:
                pass

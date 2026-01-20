import customtkinter as ctk
import json
import queue
import re
import threading
from datetime import datetime
from pathlib import Path
from tkinter import filedialog, messagebox
from urllib import request
from urllib.error import URLError
from urllib.parse import urlparse
from database import DatabaseManager
from captcha_solver import CaptchaSolver
from discord_worker import DiscordWorker
from scraper import DiscordScraper
from status_changer import StatusChanger
from joiner import DiscordJoiner # Import nowego moduĹ‚u
from token_manager import TokenManager
from updater import UpdateManager, UpdateError
from metrics import HealthMetrics

ctk.set_appearance_mode("Dark")
ctk.set_default_color_theme("blue")

APP_VERSION = "1.0"
DEFAULT_VERSION_ENDPOINT = "https://updates.example.com/massdm/version.json"
TRUSTED_UPDATE_HOSTS = {"updates.example.com"}

class MassDMApp(ctk.CTk):
    def __init__(self):
        super().__init__()

        self.db = DatabaseManager()
        self.db.reset_daily_counters()
        self.module_vars = {
            "dm": ctk.BooleanVar(value=self._get_setting_bool("module_dm", True)),
            "joiner": ctk.BooleanVar(value=self._get_setting_bool("module_joiner", True)),
            "scraper": ctk.BooleanVar(value=self._get_setting_bool("module_scraper", True)),
            "status": ctk.BooleanVar(value=self._get_setting_bool("module_status", True)),
            "captcha": ctk.BooleanVar(value=self._get_setting_bool("module_captcha", True)),
        }
        self.export_banned_tokens_plaintext_var = ctk.BooleanVar(
            value=self._get_setting_bool("export_banned_tokens_plaintext", False)
        )
        self.log_queue = queue.Queue()
        self.metrics = HealthMetrics()
        self.worker = DiscordWorker(self.db, self.add_log, self.metrics)
        self.scraper = DiscordScraper(self.db, self.add_log, self.metrics)
        self.status_changer = StatusChanger(self.db, self.add_log, self.metrics)
        self.captcha_solver = CaptchaSolver(self.db, self.add_log)
        self.joiner = DiscordJoiner(self.db, self.add_log, self.captcha_solver, self.metrics) # Inicjalizacja
        self.token_manager = TokenManager(self.db, self.add_log, self.metrics)
        self.log_entries = []
        self.error_entries = []
        self.max_log_entries = 2000
        self.max_error_entries = 2000
        self.unverified_retry_delay_seconds = 60
        self.max_unverified_retries = 3
        self.log_filter_var = ctk.StringVar()
        self.error_filter_var = ctk.StringVar()
        self.log_level_var = ctk.StringVar(value="All")
        self.logs_dir = Path("logs")
        self.logs_dir.mkdir(parents=True, exist_ok=True)
        self.log_file_path = self.logs_dir / f"app_{datetime.now().strftime('%Y%m%d')}.log"
        self.error_log_file_path = self.logs_dir / f"errors_{datetime.now().strftime('%Y%m%d')}.log"
        self.banlist_path = Path("banned_dead_tokens.txt")

        self.title(f"Mass-DM Farm Tool Pro v{APP_VERSION}")
        self.geometry("1100x1000") # ZwiÄ™kszona wysokoĹ›Ä‡ na nowÄ… sekcjÄ™

        self.grid_columnconfigure(1, weight=1)
        self.grid_rowconfigure(0, weight=1)
        self.settings_window = None
        self.version_status_var = ctk.StringVar(value="Last check: --")
        self.update_status_var = ctk.StringVar(value="Update: --")
        self.update_info = None
        self.manual_update_requested = False
        self.notification_var = ctk.StringVar(value="Powiadomienia: --")

        # --- SIDEBAR ---
        self.sidebar = ctk.CTkFrame(self, width=200, corner_radius=0)
        self.sidebar.grid(row=0, column=0, sticky="nsew")
        self.logo = ctk.CTkLabel(self.sidebar, text="FARM TOOL", font=ctk.CTkFont(size=24, weight="bold"))
        self.logo.grid(row=0, column=0, padx=20, pady=30)
        self.btn_settings = ctk.CTkButton(self.sidebar, text="Open Settings", command=self.open_settings_window)
        self.btn_settings.grid(row=1, column=0, padx=20, pady=10)

        # --- MAIN CONTENT AREA ---
        self.main_container = ctk.CTkFrame(self, fg_color="transparent")
        self.main_container.grid(row=0, column=1, padx=20, pady=20, sticky="nsew")

        # 0. SEKCJA POWIADOMIEĹ
        self.notification_frame = ctk.CTkFrame(self.main_container)
        self.notification_frame.pack(fill="x", pady=(0, 10))
        self.notification_label = ctk.CTkLabel(
            self.notification_frame,
            textvariable=self.notification_var,
            anchor="w",
        )
        self.notification_label.pack(side="left", padx=10, pady=8, fill="x", expand=True)
        self.notification_clear_btn = ctk.CTkButton(
            self.notification_frame,
            text="Clear",
            width=80,
            command=self.clear_notification,
        )
        self.notification_clear_btn.pack(side="right", padx=10, pady=8)

        # 0.5 HEALTH SECTION
        self.health_frame = ctk.CTkFrame(self.main_container)
        self.health_frame.pack(fill="x", pady=(0, 10))
        for col in range(4):
            self.health_frame.grid_columnconfigure(col, weight=1)
        ctk.CTkLabel(self.health_frame, text="Health", font=ctk.CTkFont(size=16, weight="bold")).grid(
            row=0, column=0, columnspan=4, pady=10
        )
        self.health_uptime_label = ctk.CTkLabel(self.health_frame, text="Uptime: --", anchor="w")
        self.health_uptime_label.grid(row=1, column=0, padx=10, pady=5, sticky="w")
        self.health_avg_label = ctk.CTkLabel(self.health_frame, text="Avg request: --", anchor="w")
        self.health_avg_label.grid(row=1, column=1, padx=10, pady=5, sticky="w")
        self.health_rate_label = ctk.CTkLabel(self.health_frame, text="Rate limits: --", anchor="w")
        self.health_rate_label.grid(row=1, column=2, padx=10, pady=5, sticky="w")
        self.health_requests_label = ctk.CTkLabel(self.health_frame, text="Requests: --", anchor="w")
        self.health_requests_label.grid(row=1, column=3, padx=10, pady=5, sticky="w")

        self.workflow_frame = ctk.CTkFrame(self.main_container)
        self.workflow_frame.pack(fill="x", pady=(0, 10))
        for col in range(2):
            self.workflow_frame.grid_columnconfigure(col, weight=1)
        ctk.CTkLabel(self.workflow_frame, text="Start Steps", font=ctk.CTkFont(size=16, weight="bold")).grid(
            row=0, column=0, columnspan=2, pady=10
        )
        self.step_accounts_label = ctk.CTkLabel(self.workflow_frame, text="1. Konta: --", anchor="w")
        self.step_accounts_label.grid(row=1, column=0, padx=10, pady=2, sticky="w")
        self.step_proxies_label = ctk.CTkLabel(self.workflow_frame, text="2. Proxy: --", anchor="w")
        self.step_proxies_label.grid(row=2, column=0, padx=10, pady=2, sticky="w")
        self.step_invites_label = ctk.CTkLabel(self.workflow_frame, text="3. Serwery: --", anchor="w")
        self.step_invites_label.grid(row=3, column=0, padx=10, pady=2, sticky="w")
        self.step_templates_label = ctk.CTkLabel(self.workflow_frame, text="4. Templates: --", anchor="w")
        self.step_templates_label.grid(row=4, column=0, padx=10, pady=2, sticky="w")
        self.step_settings_label = ctk.CTkLabel(self.workflow_frame, text="5. Ustawienia: --", anchor="w")
        self.step_settings_label.grid(row=5, column=0, padx=10, pady=2, sticky="w")

        self.workflow_refresh_btn = ctk.CTkButton(self.workflow_frame, text="Refresh", command=self.refresh_workflow_status)
        self.workflow_refresh_btn.grid(row=1, column=1, padx=10, pady=5, sticky="e")
        self.workflow_next_btn = ctk.CTkButton(self.workflow_frame, text="Dalej", command=self.advance_workflow)
        self.workflow_next_btn.grid(row=2, column=1, padx=10, pady=5, sticky="e")

        self.workflow_actions_frame = ctk.CTkFrame(self.main_container)
        self.workflow_actions_frame.pack(fill="x", pady=(0, 10))
        for col in range(3):
            self.workflow_actions_frame.grid_columnconfigure(col, weight=1)
        ctk.CTkLabel(self.workflow_actions_frame, text="Actions", font=ctk.CTkFont(size=16, weight="bold")).grid(
            row=0, column=0, columnspan=3, pady=10
        )
        self.join_action_btn = ctk.CTkButton(
            self.workflow_actions_frame,
            text="DOLACZ",
            fg_color="#f39c12",
            hover_color="#d35400",
            command=self._handle_join_action,
        )
        self.join_action_btn.grid(row=1, column=0, padx=10, pady=5, sticky="w")
        self.scrape_action_btn = ctk.CTkButton(
            self.workflow_actions_frame,
            text="SCRAP MEMBERS",
            fg_color="#16a085",
            command=self._handle_scrape_action,
        )
        self.scrape_action_btn.grid(row=1, column=1, padx=10, pady=5, sticky="w")
        self.dm_action_btn = ctk.CTkButton(
            self.workflow_actions_frame,
            text="Wyslij DM",
            fg_color="#2ecc71",
            command=self._handle_dm_action,
        )
        self.dm_action_btn.grid(row=1, column=2, padx=10, pady=5, sticky="w")
        self.scrape_action_btn.grid_remove()
        self.dm_action_btn.grid_remove()
        self._set_workflow_stage("setup")

        # 1. SEKCJA WIADOMOĹšCI (przeniesiona do ustawien)
        self.friend_request_var = ctk.BooleanVar(value=False)
        self.settings_loaded = False
        self.workflow_stage = "setup"

        # 2. SEKCJA LOGĂ“W
        self.log_frame = ctk.CTkFrame(self.main_container)
        self.log_frame.pack(fill="both", expand=True, pady=10)
        self.log_controls = ctk.CTkFrame(self.log_frame, fg_color="transparent")
        self.log_controls.pack(fill="x", padx=10, pady=(10, 0))
        ctk.CTkLabel(self.log_controls, text="Type:", text_color="#b0b0b0").pack(side="left")
        self.log_level_menu = ctk.CTkOptionMenu(
            self.log_controls,
            values=["All", "Error", "Warning", "Info"],
            variable=self.log_level_var,
        )
        self.log_level_menu.pack(side="left", padx=(5, 15))
        self.log_level_var.trace_add("write", self.apply_log_filter)
        ctk.CTkLabel(self.log_controls, text="Log filter:", text_color="#b0b0b0").pack(side="left")
        self.log_filter_input = ctk.CTkEntry(
            self.log_controls,
            textvariable=self.log_filter_var,
            placeholder_text="Type to filter logs...",
        )
        self.log_filter_input.pack(side="left", padx=10, fill="x", expand=True)
        self.log_filter_var.trace_add("write", self.apply_log_filter)
        self.log_file_label = ctk.CTkLabel(
            self.log_frame,
            text=f"Log file: {self.log_file_path}",
            text_color="#8a8a8a",
            anchor="w",
        )
        self.log_file_label.pack(fill="x", padx=10, pady=(5, 0))
        self.log_tabs = ctk.CTkTabview(self.log_frame)
        self.log_tabs.pack(fill="both", expand=True, padx=10, pady=10)
        self.logs_tab = self.log_tabs.add("Logs")
        self.errors_tab = self.log_tabs.add("Errors")
        self.log_box = ctk.CTkTextbox(self.logs_tab, height=200, fg_color="#1a1a1a")
        self.log_box.pack(fill="both", expand=True, padx=10, pady=10)
        self.error_controls = ctk.CTkFrame(self.errors_tab, fg_color="transparent")
        self.error_controls.pack(fill="x", padx=10, pady=(10, 0))
        ctk.CTkLabel(self.error_controls, text="Error filter:", text_color="#b0b0b0").pack(side="left")
        self.error_filter_input = ctk.CTkEntry(
            self.error_controls,
            textvariable=self.error_filter_var,
            placeholder_text="Type to filter errors...",
        )
        self.error_filter_input.pack(side="left", padx=10, fill="x", expand=True)
        self.error_filter_var.trace_add("write", self.apply_error_filter)
        self.error_file_label = ctk.CTkLabel(
            self.errors_tab,
            text=f"Error log file: {self.error_log_file_path}",
            text_color="#8a8a8a",
            anchor="w",
        )
        self.error_file_label.pack(fill="x", padx=10, pady=(5, 0))
        self.error_box = ctk.CTkTextbox(self.errors_tab, height=200, fg_color="#1a1a1a")
        self.error_box.pack(fill="both", expand=True, padx=10, pady=10)

        # --- BOTTOM CONTROL BAR ---
        self.control_bar = ctk.CTkFrame(self, height=80)
        self.control_bar.grid(row=1, column=1, sticky="ew", padx=20, pady=(0, 20))
        self.start_btn = ctk.CTkButton(self.control_bar, text="START MISSION", fg_color="#2ecc71", command=self.start_mission)
        self.start_btn.pack(side="left", padx=50, pady=20)
        self.start_btn.pack_forget()
        self.stop_btn = ctk.CTkButton(self.control_bar, text="STOP ALL", fg_color="#e74c3c", command=self.stop_all)
        self.stop_btn.pack(side="right", padx=50, pady=20)

        self.after(100, self.process_log_queue)
        self.after(1000, self.refresh_health_metrics)

    def add_log(self, message):
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        level = self._get_log_level(message)
        self.log_queue.put({"timestamp": timestamp, "message": message, "level": level})

    def log_error(self, message):
        self.add_log(f"BĹ‚Ä…d: {message}")

    def _get_setting_bool(self, key, default=True):
        value = self.db.get_setting(key, None)
        if value in (None, ""):
            return default
        if isinstance(value, str):
            return value.strip().lower() in {"1", "true", "yes", "on"}
        return bool(value)

    def _set_setting_bool(self, key, value):
        self.db.set_setting(key, "true" if value else "false")

    def _get_setting_number(self, key, default):
        value = self.db.get_setting(key, None)
        if value in (None, ""):
            return default
        try:
            return float(value)
        except (TypeError, ValueError):
            return default

    def _parse_delay_range(self, min_input, max_input, label, cast_type=int, min_value=0.0):
        min_raw = min_input.get().strip()
        max_raw = max_input.get().strip()
        try:
            min_val = cast_type(min_raw)
        except (TypeError, ValueError):
            self.log_error(f"{label} (min) musi byÄ‡ liczbÄ….")
            return None
        try:
            max_val = cast_type(max_raw)
        except (TypeError, ValueError):
            self.log_error(f"{label} (max) musi byÄ‡ liczbÄ….")
            return None
        if min_val < min_value or max_val < min_value:
            self.log_error(f"{label} musi byÄ‡ >= {min_value}.")
            return None
        if min_val > max_val:
            self.log_error(f"{label} min nie moĹĽe byÄ‡ wiÄ™kszy od max.")
            return None
        return min_val, max_val

    def _parse_min_value(self, input_widget, label, cast_type=int, min_value=0.0):
        raw = input_widget.get().strip()
        try:
            value = cast_type(raw)
        except (TypeError, ValueError):
            self.log_error(f"{label} musi byÄ‡ liczbÄ….")
            return None
        if value < min_value:
            self.log_error(f"{label} musi byÄ‡ >= {min_value}.")
            return None
        return value

    def _count_templates(self):
        if not hasattr(self, "msg_input"):
            return 0
        raw = self.msg_input.get("1.0", "end").strip()
        if not raw:
            return 0
        templates = [tpl.strip() for tpl in re.split(r"\n-{3,}\n", raw) if tpl.strip()]
        return len(templates)

    def _count_invites(self):
        if not hasattr(self, "invite_input"):
            return 0
        raw_text = self.invite_input.get("1.0", "end")
        lines = [line.strip() for line in raw_text.splitlines() if line.strip()]
        count = 0
        for line in lines:
            if self.normalize_invite(line):
                count += 1
        return count

    def refresh_workflow_status(self):
        accounts = self.db.get_accounts_overview()
        account_count = len(accounts)
        proxy_count = sum(1 for acc in accounts if acc[2])
        invite_count = self._count_invites()
        template_count = self._count_templates()
        settings_status = "tak" if self.settings_loaded else "nie"

        self.step_accounts_label.configure(text=f"1. Konta: {account_count}")
        self.step_proxies_label.configure(text=f"2. Proxy: {proxy_count}")
        self.step_invites_label.configure(text=f"3. Serwery: {invite_count}")
        self.step_templates_label.configure(text=f"4. Templates: {template_count}")
        self.step_settings_label.configure(text=f"5. Ustawienia: {settings_status}")

        can_next = account_count > 0 and invite_count > 0 and template_count > 0 and self.settings_loaded
        self.workflow_next_btn.configure(state="normal" if can_next else "disabled")

    def advance_workflow(self):
        self._set_workflow_stage("join")

    def _set_workflow_stage(self, stage):
        self.workflow_stage = stage
        joiner_enabled = self.module_vars["joiner"].get()
        scraper_enabled = self.module_vars["scraper"].get()
        dm_enabled = self.module_vars["dm"].get()
        if stage == "setup":
            self.join_action_btn.configure(state="disabled")
            self.scrape_action_btn.grid_remove()
            self.dm_action_btn.grid_remove()
            return
        if stage == "join":
            self.join_action_btn.configure(state="normal" if joiner_enabled else "disabled")
            self.scrape_action_btn.grid_remove()
            self.dm_action_btn.grid_remove()
            return
        if stage == "scrape":
            self.join_action_btn.configure(state="disabled")
            self.scrape_action_btn.grid()
            self.scrape_action_btn.configure(state="normal" if scraper_enabled else "disabled")
            self.dm_action_btn.grid_remove()
            return
        if stage == "dm":
            self.join_action_btn.configure(state="disabled")
            self.scrape_action_btn.configure(state="disabled")
            self.dm_action_btn.grid()
            self.dm_action_btn.configure(state="normal" if dm_enabled else "disabled")

    def _handle_join_action(self):
        if not hasattr(self, "invite_input"):
            self.log_error("OtwĂłrz ustawienia i wprowadĹş zaproszenia do serwerĂłw.")
            return
        self.join_action_btn.configure(state="disabled")
        started = self.start_joining(on_complete=self._on_join_complete)
        if not started:
            self.join_action_btn.configure(state="normal")

    def _on_join_complete(self, success):
        def _update():
            if success:
                self._set_workflow_stage("scrape")
            else:
                self.log_error("DoĹ‚Ä…czanie nie powiodĹ‚o siÄ™ lub przerwane.")
                self.join_action_btn.configure(state="normal")
        self.after(0, _update)

    def _handle_scrape_action(self):
        self.scrape_action_btn.configure(state="disabled")
        started = self.start_guild_scraping(on_complete=self._on_scrape_complete)
        if not started:
            self.scrape_action_btn.configure(state="normal")

    def _on_scrape_complete(self, success):
        def _update():
            if success:
                self._set_workflow_stage("dm")
            else:
                self.log_error("Scrapowanie nie powiodĹ‚o siÄ™ lub przerwane.")
                self.scrape_action_btn.configure(state="normal")
        self.after(0, _update)

    def _handle_dm_action(self):
        self.start_mission()

    def _normalize_version(self, version_value):
        if not version_value:
            return ()
        parts = re.findall(r"\d+", str(version_value))
        return tuple(int(part) for part in parts)

    def _compare_versions(self, current_version, latest_version):
        current_parts = self._normalize_version(current_version)
        latest_parts = self._normalize_version(latest_version)
        if not current_parts or not latest_parts:
            return None
        max_len = max(len(current_parts), len(latest_parts))
        current_parts = current_parts + (0,) * (max_len - len(current_parts))
        latest_parts = latest_parts + (0,) * (max_len - len(latest_parts))
        if current_parts == latest_parts:
            return 0
        return 1 if current_parts > latest_parts else -1

    def _set_version_status(self, text):
        self.version_status_var.set(text)

    def _validate_update_endpoint(self, endpoint: str):
        parsed = urlparse(endpoint)
        if parsed.scheme != "https":
            return False, "Endpoint wersji musi używać https."
        host = parsed.hostname
        if not host:
            return False, "Nieprawidłowy host endpointu wersji."
        if host not in TRUSTED_UPDATE_HOSTS:
            return False, f"Nieufny host endpointu: {host}."
        return True, None

    def _set_update_status(self, text):
        self.update_status_var.set(text)

    def _set_update_button_state(self, enabled):
        if hasattr(self, "update_download_btn"):
            self.update_download_btn.configure(state="normal" if enabled else "disabled")
        if hasattr(self, "manual_update_btn"):
            self.manual_update_btn.configure(state="normal" if enabled else "disabled")

    def show_notification(self, message, level="info"):
        colors = {
            "info": "#8a8a8a",
            "success": "#2ecc71",
            "warning": "#f39c12",
            "error": "#e74c3c",
        }
        self.notification_var.set(message)
        self.notification_label.configure(text_color=colors.get(level, "#8a8a8a"))

    def clear_notification(self):
        self.notification_var.set("Powiadomienia: --")
        self.notification_label.configure(text_color="#8a8a8a")

    def _format_duration(self, total_seconds):
        total_seconds = int(max(0, total_seconds))
        hours = total_seconds // 3600
        minutes = (total_seconds % 3600) // 60
        seconds = total_seconds % 60
        if hours > 0:
            return f"{hours}h {minutes:02d}m {seconds:02d}s"
        if minutes > 0:
            return f"{minutes}m {seconds:02d}s"
        return f"{seconds}s"

    def refresh_health_metrics(self):
        data = self.metrics.snapshot()
        uptime = self._format_duration(data["uptime_seconds"])
        avg_ms = data["avg_request_ms"]
        rate_count = data["rate_limit_count"]
        last_rate = data["last_rate_limit_age_seconds"]

        self.health_uptime_label.configure(text=f"Uptime: {uptime}")
        self.health_avg_label.configure(text=f"Avg request: {avg_ms:.1f} ms")
        if last_rate is not None:
            last_text = self._format_duration(last_rate)
            self.health_rate_label.configure(text=f"Rate limits: {rate_count} (last {last_text} ago)")
        else:
            self.health_rate_label.configure(text=f"Rate limits: {rate_count}")
        self.health_requests_label.configure(text=f"Requests: {data['total_requests']}")

        self.after(1000, self.refresh_health_metrics)

    def save_version_settings(self):
        endpoint = self.version_endpoint_input.get().strip()
        if endpoint:
            ok, err = self._validate_update_endpoint(endpoint)
            if not ok:
                self.log_error(err)
                self.show_notification(err, level="error")
                return
            self.db.set_setting("version_endpoint", endpoint)
            self.add_log("[Settings] Zapisano endpoint wersji.")
        else:
            self.db.set_setting("version_endpoint", None)
            self.add_log("[Settings] Usunięto endpoint wersji.")

    def check_for_updates(self):
        endpoint = self.version_endpoint_input.get().strip()
        if not endpoint:
            self.log_error("Endpoint wersji jest pusty. Uzupełnij go w ustawieniach.")
            self.show_notification("Brak endpointu wersji. Uzupełnij ustawienia.", level="error")
            return
        ok, err = self._validate_update_endpoint(endpoint)
        if not ok:
            self.log_error(err)
            self.show_notification(err, level="error")
            return
        self.db.set_setting("version_endpoint", endpoint)
        self.add_log("[Updater] Sprawdzanie najnowszej wersji...")
        self.show_notification("Sprawdzanie aktualizacji...", level="info")
        threading.Thread(
            target=self._check_for_updates_worker,
            args=(endpoint,),
            daemon=True,
        ).start()

    def manual_update_with_prompt(self):
        self.manual_update_requested = True
        self.check_for_updates()

    def _check_for_updates_worker(self, endpoint):
        try:
            with request.urlopen(endpoint, timeout=10) as response:
                payload = response.read().decode("utf-8")
            data = json.loads(payload)
        except (URLError, json.JSONDecodeError, UnicodeDecodeError) as exc:
            self.add_log(f"[Updater] Nie udało się pobrać wersji: {exc}")
            self.after(0, lambda: self._set_version_status("Last check: error"))
            self.after(0, lambda: self._handle_update_error("Błąd pobierania informacji o wersji."))
            return

        updater = UpdateManager(Path(__file__).resolve().parent, self.add_log)
        try:
            updater._require_valid_signature(data)
        except UpdateError as exc:
            self.add_log(f"[Updater] Podpis nieprawidłowy: {exc}")
            self.after(0, lambda: self._set_version_status("Last check: invalid signature"))
            self.after(0, lambda: self._handle_update_error("Nieprawidłowy podpis aktualizacji."))
            return

        latest = data.get("latest_version") or data.get("version")
        download_url = data.get("download_url") or data.get("url")
        files_payload = data.get("files")
        if not latest:
            self.add_log("[Updater] Brak pola latest_version/version w odpowiedzi.")
            self.after(0, lambda: self._set_version_status("Last check: invalid response"))
            return

        comparison = self._compare_versions(APP_VERSION, latest)
        if comparison is None:
            message = f"[Updater] Bieżąca wersja: {APP_VERSION}, najnowsza: {latest}."
        elif comparison < 0:
            message = f"[Updater] Dostępna aktualizacja: {latest} (obecnie {APP_VERSION})."
        elif comparison == 0:
            message = f"[Updater] Masz najnowszą wersję ({APP_VERSION})."
        else:
            message = f"[Updater] Wersja lokalna ({APP_VERSION}) jest nowsza niż {latest}."
        if download_url:
            message += f" Link: {download_url}"
        self.add_log(message)
        update_available = comparison is not None and comparison < 0 and (download_url or files_payload)
        if update_available:
            self.update_info = data
            self.after(0, lambda: self._set_update_button_state(True))
            self.after(0, lambda: self._set_update_status("Update: dostępna"))
        else:
            self.update_info = None
            self.after(0, lambda: self._set_update_button_state(False))
            self.after(0, lambda: self._set_update_status("Update: brak"))
        self.after(0, lambda: self._set_version_status(f"Last check: {latest}"))
        self.after(0, lambda: self._handle_update_result(latest, update_available))

    def _handle_update_result(self, latest, update_available):
        if update_available:
            self.show_notification(
                f"Dostępna aktualizacja ({latest}). Możesz ją pobrać ręcznie.",
                level="warning",
            )
        else:
            self.show_notification("Brak nowych aktualizacji.", level="success")

        if self.manual_update_requested:
            self.manual_update_requested = False
            if update_available:
                confirmed = messagebox.askyesno(
                    "Aktualizacja",
                    f"Dostępna jest aktualizacja {latest}. Czy pobrać i zainstalować teraz?",
                )
                if confirmed:
                    self.download_update()
            else:
                messagebox.showinfo("Aktualizacja", "Brak dostępnych aktualizacji.")

    def _handle_update_error(self, message):
        self.show_notification(message, level="error")
        if self.manual_update_requested:
            self.manual_update_requested = False
            messagebox.showerror("Aktualizacja", message)

    def download_update(self):
        endpoint = self.version_endpoint_input.get().strip()
        if not endpoint:
            self.log_error("Endpoint wersji jest pusty. Uzupełnij go w ustawieniach.")
            self.show_notification("Brak endpointu wersji. Uzupełnij ustawienia.", level="error")
            return
        ok, err = self._validate_update_endpoint(endpoint)
        if not ok:
            self.log_error(err)
            self.show_notification(err, level="error")
            return
        self.db.set_setting("version_endpoint", endpoint)
        self.add_log("[Updater] Pobieranie aktualizacji...")
        self._set_update_button_state(False)
        self._set_update_status("Update: pobieranie...")
        self.show_notification("Pobieranie aktualizacji...", level="info")
        threading.Thread(
            target=self._download_update_worker,
            args=(endpoint,),
            daemon=True,
        ).start()

    def _download_update_worker(self, endpoint):
        try:
            with request.urlopen(endpoint, timeout=10) as response:
                payload = response.read().decode("utf-8")
            data = json.loads(payload)
        except (URLError, json.JSONDecodeError, UnicodeDecodeError) as exc:
            self.add_log(f"[Updater] Nie udaĹ‚o siÄ™ pobraÄ‡ aktualizacji: {exc}")
            self.after(0, lambda: self._set_update_status("Update: bĹ‚Ä…d pobierania"))
            self.after(0, lambda: self._set_update_button_state(True))
            self.after(0, lambda: self.show_notification("BĹ‚Ä…d pobierania aktualizacji.", level="error"))
            return

        updater = UpdateManager(Path(__file__).resolve().parent, self.add_log)
        try:
            updater.download_and_apply(data)
        except UpdateError as exc:
            self.add_log(f"[Updater] Aktualizacja nieudana: {exc}")
            self.after(0, lambda: self._set_update_status("Update: bĹ‚Ä…d walidacji"))
            self.after(0, lambda: self._set_update_button_state(True))
            self.after(0, lambda: self.show_notification("Aktualizacja nieudana.", level="error"))
            return

        self.add_log("[Updater] Aktualizacja zakoĹ„czona. Uruchom ponownie aplikacjÄ™.")
        self.after(0, lambda: self._set_update_status("Update: zainstalowana"))
        self.after(0, lambda: self.show_notification("Aktualizacja zainstalowana. Uruchom ponownie aplikacjÄ™.", level="success"))

    def validate_token_format(self, token):
        if not token:
            self.log_error("Niepoprawny token: puste pole.")
            return False
        token_pattern = re.compile(r"^[A-Za-z0-9_\-]+\.[A-Za-z0-9_\-]+\.[A-Za-z0-9_\-]+$")
        if not token_pattern.match(token) or len(token) < 50:
            self.log_error("Niepoprawny format tokena.")
            return False
        return True

    def validate_proxy(self, proxy):
        if not proxy:
            return True
        parsed = urlparse(proxy)
        if parsed.scheme and parsed.hostname and parsed.port:
            if parsed.scheme not in {"http", "https", "socks5"}:
                self.log_error("Niepoprawny format proxy.")
                return False
            return True
        parsed = urlparse(f"http://{proxy}")
        if parsed.hostname and parsed.port:
            return True
        self.log_error("Niepoprawny format proxy.")
        return False

    def normalize_invite(self, invite):
        invite = invite.strip()
        if not invite:
            return None
        url_match = re.match(
            r"^(?:https?://)?(?:www\.)?(?:discord\.gg|discord(?:app)?\.com/invite)/([A-Za-z0-9-]+)$",
            invite,
        )
        if url_match:
            return url_match.group(1)
        if re.match(r"^[A-Za-z0-9-]{2,}$", invite):
            return invite
        return None

    def is_valid_channel_id(self, channel_id):
        if not channel_id:
            self.log_error("Niepoprawny Channel ID: puste pole.")
            return False
        if not re.match(r"^\d{17,20}$", channel_id):
            self.log_error("Niepoprawny format Channel ID.")
            return False
        return True

    def _get_scrape_limit(self, raw_value, default_limit):
        value = (raw_value or "").strip()
        if not value:
            return default_limit
        try:
            limit = int(value)
        except ValueError:
            self.log_error("Niepoprawny format Range. Użyj liczby całkowitej.")
            return None
        if limit <= 0:
            self.log_error("Range musi być większy od zera.")
            return None
        return limit
    def is_valid_guild_id(self, guild_id):
        if not guild_id:
            self.log_error("Niepoprawny Guild ID: puste pole.")
            return False
        if not re.match(r"^\d{17,20}$", guild_id):
            self.log_error("Niepoprawny format Guild ID.")
            return False
        return True

    def process_log_queue(self):
        trimmed_logs = False
        trimmed_errors = False
        try:
            while True:
                entry = self.log_queue.get_nowait()
                self.log_entries.append(entry)
                if len(self.log_entries) > self.max_log_entries:
                    overflow = len(self.log_entries) - self.max_log_entries
                    del self.log_entries[:overflow]
                    trimmed_logs = True
                self._write_log_to_file(entry)
                if self._matches_log_filter(entry):
                    self._append_log_entry(entry)
                if self._is_error_entry(entry):
                    self.error_entries.append(entry)
                    if len(self.error_entries) > self.max_error_entries:
                        overflow = len(self.error_entries) - self.max_error_entries
                        del self.error_entries[:overflow]
                        trimmed_errors = True
                    self._write_error_log_to_file(entry)
                    if self._matches_error_filter(entry):
                        self._append_error_entry(entry)
        except queue.Empty:
            pass
        if trimmed_logs:
            self.apply_log_filter()
        if trimmed_errors:
            self.apply_error_filter()
        self.after(100, self.process_log_queue)

    def _append_log_entry(self, entry):
        self.log_box.insert("end", f"[{entry['timestamp']}] {entry['message']}\n")
        self.log_box.see("end")

    def _append_error_entry(self, entry):
        self.error_box.insert("end", f"[{entry['timestamp']}] {entry['message']}\n")
        self.error_box.see("end")

    def _is_error_entry(self, entry):
        return entry["level"] == "Error"

    def _get_log_level(self, message):
        normalized = message.strip().casefold()
        if "bĹ‚Ä…d" in normalized or normalized.startswith("[!]"):
            return "Error"
        if normalized.startswith("ostrzeĹĽenie") or normalized.startswith("warning"):
            return "Warning"
        return "Info"

    def _matches_log_filter(self, entry):
        filter_text = self.log_filter_var.get().strip().lower()
        level_filter = self.log_level_var.get().strip().casefold()
        if level_filter and level_filter != "all":
            if entry["level"].casefold() != level_filter:
                return False
        if not filter_text:
            return True
        return filter_text in entry["message"].lower() or filter_text in entry["timestamp"].lower()

    def apply_log_filter(self, *_args):
        self.log_box.delete("1.0", "end")
        for entry in self.log_entries:
            if self._matches_log_filter(entry):
                self._append_log_entry(entry)

    def _matches_error_filter(self, entry):
        filter_text = self.error_filter_var.get().strip().lower()
        if not filter_text:
            return True
        return filter_text in entry["message"].lower() or filter_text in entry["timestamp"].lower()

    def apply_error_filter(self, *_args):
        self.error_box.delete("1.0", "end")
        for entry in self.error_entries:
            if self._matches_error_filter(entry):
                self._append_error_entry(entry)

    def _write_log_to_file(self, entry):
        try:
            with self.log_file_path.open("a", encoding="utf-8") as log_file:
                log_file.write(f"[{entry['timestamp']}] {entry['message']}\n")
        except OSError:
            pass

    def _write_error_log_to_file(self, entry):
        try:
            with self.error_log_file_path.open("a", encoding="utf-8") as log_file:
                log_file.write(f"[{entry['timestamp']}] {entry['message']}\n")
        except OSError:
            pass

    def add_account(self):
        token = self.token_input.get().strip()
        proxy = self.proxy_input.get().strip()
        dm_limit_raw = self.dm_limit_input.get().strip()
        join_limit_raw = self.join_limit_input.get().strip()
        if not self.validate_token_format(token):
            return
        if not self.validate_proxy(proxy):
            return
        try:
            dm_limit = int(dm_limit_raw) if dm_limit_raw else 15
        except ValueError:
            self.log_error("Limit DM musi byÄ‡ liczbÄ… caĹ‚kowitÄ….")
            return
        try:
            join_limit = int(join_limit_raw) if join_limit_raw else 5
        except ValueError:
            self.log_error("Limit joinĂłw musi byÄ‡ liczbÄ… caĹ‚kowitÄ….")
            return
        if dm_limit <= 0 or join_limit <= 0:
            self.log_error("Limity muszÄ… byÄ‡ wiÄ™ksze od zera.")
            return
        self.add_acc_btn.configure(state="disabled")
        thread = threading.Thread(
            target=self._add_account_worker,
            args=(token, proxy, dm_limit, join_limit),
            daemon=True,
        )
        thread.start()

    def _add_account_worker(self, token, proxy, dm_limit, join_limit):
        status, info = self.token_manager.validate_token(token, proxy)
        self.after(
            0,
            lambda: self._handle_add_account_result(
                token, proxy, dm_limit, join_limit, status, info
            ),
        )

    def _handle_add_account_result(self, token, proxy, dm_limit, join_limit, status, info):
        try:
            if status == "unauthorized":
                self.log_error(f"Token niepoprawny: {info}.")
                return

            initial_status = "Active" if status == "ok" else "Unverified"
            account_id = self.db.add_account("discord", token, proxy, dm_limit, join_limit, initial_status)
            if not account_id:
                self.log_error("Konto juĹĽ istnieje lub token jest niepoprawny.")
                return

            if status == "ok":
                self.add_log(f"Account added: {info}. DM limit: {dm_limit}, Join limit: {join_limit}.")
            else:
                self.add_log(
                    f"Account added: temporary validation error ({info}). Marked as Unverified."
                )
                self._schedule_account_recheck(account_id, token, proxy, attempt=1)

            self._reset_add_account_form()
            self.refresh_accounts_overview()
        finally:
            self.add_acc_btn.configure(state="normal")

    def _schedule_account_recheck(self, account_id, token, proxy, attempt=1):
        if attempt > self.max_unverified_retries:
            self.add_log(f"[Accounts] Konto {account_id}: nadal niezweryfikowane, koniec prob.")
            return
        timer = threading.Timer(
            self.unverified_retry_delay_seconds,
            self._run_account_recheck,
            args=(account_id, token, proxy, attempt),
        )
        timer.daemon = True
        timer.start()

    def _run_account_recheck(self, account_id, token, proxy, attempt):
        status, info = self.token_manager.validate_token(token, proxy)
        self.after(
            0,
            lambda: self._handle_account_recheck_result(account_id, status, info, token, proxy, attempt),
        )

    def _handle_account_recheck_result(self, account_id, status, info, token, proxy, attempt):
        if status == "ok":
            self.db.update_account_status(account_id, "Active")
            self.add_log(f"[Accounts] Konto {account_id} zweryfikowane: {info}.")
            self.refresh_accounts_overview()
            return
        if status == "unauthorized":
            self.log_error(f"[Accounts] Konto {account_id} niepoprawne: {info}. Usuwam.")
            self.db.update_account_status(account_id, "Banned/Dead")
            self.db.remove_account(account_id)
            self.refresh_accounts_overview()
            return
        self.add_log(f"[Accounts] Konto {account_id}: weryfikacja odroczona ({info}).")
        self._schedule_account_recheck(account_id, token, proxy, attempt=attempt + 1)

    def _reset_add_account_form(self):
        self.token_input.delete(0, "end")
        self.proxy_input.delete(0, "end")
        self.dm_limit_input.delete(0, "end")
        self.dm_limit_input.insert(0, "15")
        self.join_limit_input.delete(0, "end")
        self.join_limit_input.insert(0, "5")

    def _get_invite_list(self):
        raw_text = self.invite_input.get("1.0", "end")
        lines = [line.strip() for line in raw_text.splitlines() if line.strip()]
        normalized = []
        invalid = []
        for line in lines:
            code = self.normalize_invite(line)
            if code:
                normalized.append(code)
            else:
                invalid.append(line)
        if invalid:
            self.log_error(f"Niepoprawne zaproszenia (pomijam): {', '.join(invalid)}")
        return normalized

    def start_joining(self, on_complete=None):
        if not self.module_vars["joiner"].get():
            self.log_error("ModuĹ‚ Joiner jest wyĹ‚Ä…czony.")
            return False
        invites = self._get_invite_list()
        if not invites:
            self.log_error("Brak poprawnych zaproszeĹ„.")
            return False
        join_delay = self._parse_delay_range(
            self.join_delay_min_input,
            self.join_delay_max_input,
            "Join delay",
            cast_type=int,
            min_value=0,
        )
        if not join_delay:
            return False
        join_delay_min, join_delay_max = join_delay
        self.db.set_setting("join_delay_min", str(join_delay_min))
        self.db.set_setting("join_delay_max", str(join_delay_max))
        thread = threading.Thread(
            target=self.joiner.run_mass_join,
            args=(invites, join_delay_min, join_delay_max, on_complete),
        )
        thread.daemon = True
        thread.start()
        return True

    def start_scraping(self, on_complete=None):
        if not self.module_vars["scraper"].get():
            self.log_error("Moduł Scraper jest wyłączony.")
            return False
        token = self.token_input.get().strip()
        channel_id = self.scrape_channel_input.get().strip()
        range_value = self.scrape_range_input.get().strip()
        if not self.validate_token_format(token):
            return False
        if not self.is_valid_channel_id(channel_id):
            return False
        limit = self._get_scrape_limit(range_value, 500)
        if limit is None:
            return False
        thread = threading.Thread(
            target=self.scraper.scrape_history,
            args=(token, channel_id, limit, on_complete),
        )
        thread.daemon = True
        thread.start()
        return True

    def start_guild_scraping(self, on_complete=None):
        if not self.module_vars["scraper"].get():
            self.log_error("Moduł Scraper jest wyłączony.")
            return False
        token = self.token_input.get().strip()
        guild_id = self.scrape_guild_input.get().strip()
        range_value = self.scrape_range_input.get().strip()
        if not self.validate_token_format(token):
            return False
        if not self.is_valid_guild_id(guild_id):
            return False
        limit = self._get_scrape_limit(range_value, 1000)
        if limit is None:
            return False
        thread = threading.Thread(
            target=self.scraper.scrape_guild_members,
            args=(token, guild_id, limit, on_complete),
        )
        thread.daemon = True
        thread.start()
        return True

    def start_status_update(self):
        if not self.module_vars["status"].get():
            self.log_error("ModuĹ‚ Status jest wyĹ‚Ä…czony.")
            return
        status_type = self.status_type_var.get()
        custom_text = self.status_text_input.get()
        status_delay = self._parse_delay_range(
            self.status_delay_min_input,
            self.status_delay_max_input,
            "Status delay (hours)",
            cast_type=float,
            min_value=0.1,
        )
        if not status_delay:
            return
        status_delay_min, status_delay_max = status_delay
        self.db.set_setting("status_delay_min_hours", str(status_delay_min))
        self.db.set_setting("status_delay_max_hours", str(status_delay_max))
        thread = threading.Thread(
            target=self.status_changer.run_auto_update,
            args=(status_type, custom_text, status_delay_min, status_delay_max),
        )
        thread.daemon = True
        thread.start()

    def stop_status_update(self):
        self.status_changer.stop()
        self.add_log("[Status] Automatyczna zmiana statusu zatrzymana.")

    def start_mission(self):
        if not self.module_vars["dm"].get():
            self.log_error("ModuĹ‚ DM jest wyĹ‚Ä…czony.")
            return
        if not hasattr(self, "msg_input"):
            self.log_error("OtwĂłrz ustawienia i ustaw szablony wiadomoĹ›ci.")
            return
        raw = self.msg_input.get("1.0", "end").strip()
        if not raw:
            self.log_error("Pusta wiadomoĹ›Ä‡.")
            return
        templates = [tpl.strip() for tpl in re.split(r"\n-{3,}\n", raw) if tpl.strip()]
        if not templates:
            self.log_error("Brak poprawnych szablonĂłw wiadomoĹ›ci.")
            return
        dm_delay = self._parse_delay_range(
            self.dm_delay_min_input,
            self.dm_delay_max_input,
            "DM delay",
            cast_type=int,
            min_value=0,
        )
        if not dm_delay:
            return
        friend_delay = self._parse_delay_range(
            self.friend_delay_min_input,
            self.friend_delay_max_input,
            "Friend request delay",
            cast_type=int,
            min_value=0,
        )
        if not friend_delay:
            return
        account_min_interval = self._parse_min_value(
            self.account_min_interval_input,
            "Account min interval (s)",
            cast_type=int,
            min_value=0,
        )
        if account_min_interval is None:
            return
        target_min_interval = self._parse_min_value(
            self.target_min_interval_input,
            "Target min interval (s)",
            cast_type=int,
            min_value=0,
        )
        if target_min_interval is None:
            return
        dm_delay_min, dm_delay_max = dm_delay
        friend_delay_min, friend_delay_max = friend_delay
        self.db.set_setting("dm_delay_min", str(dm_delay_min))
        self.db.set_setting("dm_delay_max", str(dm_delay_max))
        self.db.set_setting("friend_delay_min", str(friend_delay_min))
        self.db.set_setting("friend_delay_max", str(friend_delay_max))
        self.db.set_setting("account_min_interval", str(account_min_interval))
        self.db.set_setting("target_min_interval", str(target_min_interval))
        use_friend_req = self.friend_request_var.get()
        thread = threading.Thread(
            target=self.worker.run_mission,
            args=(
                templates,
                dm_delay_min,
                dm_delay_max,
                use_friend_req,
                friend_delay_min,
                friend_delay_max,
                account_min_interval,
                target_min_interval,
            ),
        )
        thread.daemon = True
        thread.start()

    def stop_all(self):
        self.worker.stop()
        self.scraper.stop()
        self.status_changer.stop()
        self.joiner.stop()
        self.add_log("All processes stopped.")

    def remove_account_by_id(self):
        raw_id = self.remove_account_input.get().strip()
        if not raw_id:
            self.log_error("Podaj ID konta do usuniÄ™cia.")
            return
        try:
            account_id = int(raw_id)
        except ValueError:
            self.log_error("ID konta musi byÄ‡ liczbÄ….")
            return
        self.db.remove_account(account_id)
        self.add_log(f"[Accounts] UsuniÄ™to konto {account_id}.")
        self.remove_account_input.delete(0, "end")
        self.refresh_accounts_overview()

    def refresh_accounts_overview(self):
        accounts = self.db.get_accounts_overview()
        self.acc_overview_box.delete("1.0", "end")
        if not accounts:
            self.acc_overview_box.insert("end", "Brak kont w bazie.\n")
            self.refresh_workflow_status()
            return
        self.acc_overview_box.insert("end", "ID | Status | DM sent/limit | Join sent/limit | Proxy\n")
        self.acc_overview_box.insert("end", "-" * 70 + "\n")
        for acc_id, status, proxy, dm_limit, sent_today, join_limit, join_today in accounts:
            proxy_value = proxy if proxy else "-"
            line = f"{acc_id} | {status} | {sent_today}/{dm_limit} | {join_today}/{join_limit} | {proxy_value}\n"
            self.acc_overview_box.insert("end", line)
        self.refresh_workflow_status()

    def reset_account_counters(self):
        self.db.reset_account_counters()
        self.add_log("[Accounts] Zresetowano liczniki dzienne.")
        self.refresh_accounts_overview()

    def _extract_user_ids(self, value):
        return re.findall(r"\d{17,20}", value)

    def _parse_user_ids(self, raw_text):
        ids = []
        invalid = []
        for line in raw_text.splitlines():
            value = line.strip()
            if not value:
                continue
            matches = self._extract_user_ids(value)
            if matches:
                ids.extend(matches)
            else:
                invalid.append(value)
        unique_ids = list(dict.fromkeys(ids))
        return unique_ids, invalid

    def _parse_user_ids_from_file(self, file_path):
        ids = []
        invalid = []
        try:
            with open(file_path, "r", encoding="utf-8") as handle:
                for line in handle:
                    value = line.strip()
                    if not value:
                        continue
                    matches = self._extract_user_ids(value)
                    if matches:
                        ids.extend(matches)
                    else:
                        invalid.append(value)
        except OSError as exc:
            self.log_error(f"Nie moĹĽna odczytaÄ‡ pliku: {exc}")
            return [], []
        unique_ids = list(dict.fromkeys(ids))
        return unique_ids, invalid

    def add_targets_from_input(self):
        raw = self.target_input.get("1.0", "end")
        ids, invalid = self._parse_user_ids(raw)
        if not ids:
            self.log_error("Brak poprawnych ID do dodania.")
            return
        if invalid:
            self.log_error(f"Niepoprawne ID (pomijam): {', '.join(invalid)}")
        self.db.add_targets(ids, "discord")
        self.add_log(f"[Targets] Dodano {len(ids)} celĂłw.")
        self.target_input.delete("1.0", "end")
        self.refresh_targets_overview()

    def import_targets_from_file(self):
        file_path = filedialog.askopenfilename(
            title="Wybierz plik .txt z ID",
            filetypes=[("Text files", "*.txt"), ("All files", "*.*")],
        )
        if not file_path:
            return
        ids, invalid = self._parse_user_ids_from_file(file_path)
        if not ids:
            self.log_error("Brak poprawnych ID w pliku.")
            return
        if invalid:
            self.log_error(f"Niepoprawne ID (pomijam): {', '.join(invalid)}")
        self.db.add_targets(ids, "discord")
        self.add_log(f"[Targets] Zaimportowano {len(ids)} celĂłw z pliku.")
        self.refresh_targets_overview()

    def clear_targets(self):
        self.db.clear_targets()
        self.add_log("[Targets] Lista celĂłw wyczyszczona.")
        self.refresh_targets_overview()

    def refresh_targets_overview(self):
        counts, total = self.db.get_target_counts()
        pending = counts.get("Pending", 0)
        sent = counts.get("Sent", 0)
        failed = counts.get("Failed", 0)
        self.target_summary_label.configure(
            text=f"Targets: {total} | Pending: {pending} | Sent: {sent} | Failed: {failed}"
        )
        targets = self.db.get_targets(limit=50)
        self.target_overview_box.delete("1.0", "end")
        if not targets:
            self.target_overview_box.insert("end", "Brak celĂłw w bazie.\n")
            return
        for user_id, status in targets:
            self.target_overview_box.insert("end", f"{user_id} | {status}\n")

    def refresh_banlist_overview(self):
        self.banlist_box.delete("1.0", "end")
        if not self.banlist_path.exists():
            self.banlist_summary_label.configure(text="Banlist: 0")
            self.banlist_box.insert("end", "Brak wpisĂłw w banliĹ›cie.\n")
            return
        try:
            lines = self.banlist_path.read_text(encoding="utf-8").splitlines()
        except OSError as exc:
            self.log_error(f"Nie moĹĽna odczytaÄ‡ banlisty: {exc}")
            self.banlist_summary_label.configure(text="Banlist: ?")
            self.banlist_box.insert("end", "Nie udaĹ‚o siÄ™ odczytaÄ‡ banlisty.\n")
            return
        entries = [line for line in lines if line.strip()]
        self.banlist_summary_label.configure(text=f"Banlist: {len(entries)}")
        if not entries:
            self.banlist_box.insert("end", "Brak wpisĂłw w banliĹ›cie.\n")
            return
        self.banlist_box.insert("end", "Timestamp | Token\n")
        self.banlist_box.insert("end", "-" * 80 + "\n")
        for entry in entries[-200:]:
            self.banlist_box.insert("end", f"{entry}\n")

    def on_module_toggle(self):
        self._set_setting_bool("module_dm", self.module_vars["dm"].get())
        self._set_setting_bool("module_joiner", self.module_vars["joiner"].get())
        self._set_setting_bool("module_scraper", self.module_vars["scraper"].get())
        self._set_setting_bool("module_status", self.module_vars["status"].get())
        self._set_setting_bool("module_captcha", self.module_vars["captcha"].get())
        self.apply_module_states()

    def on_export_plaintext_toggle(self):
        self._set_setting_bool(
            "export_banned_tokens_plaintext",
            self.export_banned_tokens_plaintext_var.get(),
        )
        status = "włączony" if self.export_banned_tokens_plaintext_var.get() else "wyłączony"
        self.add_log(f"[Settings] Eksport banlisty plaintext: {status}.")

    def apply_module_states(self):
        dm_enabled = self.module_vars["dm"].get()
        joiner_enabled = self.module_vars["joiner"].get()
        scraper_enabled = self.module_vars["scraper"].get()
        status_enabled = self.module_vars["status"].get()
        captcha_enabled = self.module_vars["captcha"].get()

        if hasattr(self, "start_btn"):
            self.start_btn.configure(state="normal" if dm_enabled else "disabled")
        if hasattr(self, "friend_request_toggle"):
            self.friend_request_toggle.configure(state="normal" if dm_enabled else "disabled")
        if hasattr(self, "join_btn"):
            self.join_btn.configure(state="normal" if joiner_enabled else "disabled")
        if hasattr(self, "scrape_btn"):
            self.scrape_btn.configure(state="normal" if scraper_enabled else "disabled")
        if hasattr(self, "scrape_guild_btn"):
            self.scrape_guild_btn.configure(state="normal" if scraper_enabled else "disabled")
        if hasattr(self, "update_status_btn"):
            self.update_status_btn.configure(state="normal" if status_enabled else "disabled")
        if hasattr(self, "stop_status_btn"):
            self.stop_status_btn.configure(state="normal" if status_enabled else "disabled")
        if hasattr(self, "captcha_save_btn"):
            self.captcha_save_btn.configure(state="normal" if captcha_enabled else "disabled")
        if hasattr(self, "captcha_test_btn"):
            self.captcha_test_btn.configure(state="normal" if captcha_enabled else "disabled")
        if hasattr(self, "captcha_provider"):
            self.captcha_provider.configure(state="normal" if captcha_enabled else "disabled")
        if hasattr(self, "captcha_key_input"):
            self.captcha_key_input.configure(state="normal" if captcha_enabled else "disabled")
        if hasattr(self, "workflow_stage"):
            self._set_workflow_stage(self.workflow_stage)

    def open_settings_window(self):
        if self.settings_window and self.settings_window.winfo_exists():
            self.settings_window.focus()
            return
        self.settings_window = ctk.CTkToplevel(self)
        self.settings_window.title("Configuration")
        self.settings_window.geometry("900x900")
        self.settings_window.grid_columnconfigure(0, weight=1)
        self.settings_window.grid_rowconfigure(0, weight=1)
        self.settings_container = ctk.CTkScrollableFrame(self.settings_window, fg_color="transparent")
        self.settings_container.grid(row=0, column=0, padx=20, pady=20, sticky="nsew")
        self._build_settings_sections(self.settings_container)
        self.settings_loaded = True
        self.refresh_workflow_status()
        self.settings_window.protocol("WM_DELETE_WINDOW", self.close_settings_window)

    def close_settings_window(self):
        if self.settings_window and self.settings_window.winfo_exists():
            self.save_delay_settings()
            self.settings_window.destroy()
            self.refresh_workflow_status()
        self.settings_window = None

    def save_delay_settings(self):
        dm_delay = self._parse_delay_range(
            self.dm_delay_min_input,
            self.dm_delay_max_input,
            "DM delay",
            cast_type=int,
            min_value=0,
        )
        if not dm_delay:
            return
        join_delay = self._parse_delay_range(
            self.join_delay_min_input,
            self.join_delay_max_input,
            "Join delay",
            cast_type=int,
            min_value=0,
        )
        if not join_delay:
            return
        friend_delay = self._parse_delay_range(
            self.friend_delay_min_input,
            self.friend_delay_max_input,
            "Friend request delay",
            cast_type=int,
            min_value=0,
        )
        if not friend_delay:
            return
        status_delay = self._parse_delay_range(
            self.status_delay_min_input,
            self.status_delay_max_input,
            "Status delay (hours)",
            cast_type=float,
            min_value=0.1,
        )
        if not status_delay:
            return
        account_interval = self._parse_min_value(
            self.account_min_interval_input,
            "Account min interval (s)",
            cast_type=int,
            min_value=0,
        )
        if account_interval is None:
            return
        target_interval = self._parse_min_value(
            self.target_min_interval_input,
            "Target min interval (s)",
            cast_type=int,
            min_value=0,
        )
        if target_interval is None:
            return
        dm_delay_min, dm_delay_max = dm_delay
        join_delay_min, join_delay_max = join_delay
        friend_delay_min, friend_delay_max = friend_delay
        status_delay_min, status_delay_max = status_delay
        self.db.set_setting("dm_delay_min", str(dm_delay_min))
        self.db.set_setting("dm_delay_max", str(dm_delay_max))
        self.db.set_setting("join_delay_min", str(join_delay_min))
        self.db.set_setting("join_delay_max", str(join_delay_max))
        self.db.set_setting("friend_delay_min", str(friend_delay_min))
        self.db.set_setting("friend_delay_max", str(friend_delay_max))
        self.db.set_setting("status_delay_min_hours", str(status_delay_min))
        self.db.set_setting("status_delay_max_hours", str(status_delay_max))
        self.db.set_setting("account_min_interval", str(account_interval))
        self.db.set_setting("target_min_interval", str(target_interval))
        self.add_log("[Settings] Zapisano ustawienia delay.")

    def _build_settings_sections(self, parent):
        # 1. SEKCA KONFIGURACJI (Konta + Proxy)
        self.acc_frame = ctk.CTkFrame(parent)
        self.acc_frame.pack(fill="x", pady=10)
        ctk.CTkLabel(self.acc_frame, text="Account & Proxy Management", font=ctk.CTkFont(size=16, weight="bold")).grid(row=0, column=0, columnspan=2, pady=10)
        self.token_input = ctk.CTkEntry(self.acc_frame, placeholder_text="Discord Token", width=350)
        self.token_input.grid(row=1, column=0, padx=10, pady=5)
        self.proxy_input = ctk.CTkEntry(self.acc_frame, placeholder_text="Proxy (http://user:pass@ip:port)", width=350)
        self.proxy_input.grid(row=2, column=0, padx=10, pady=5)
        self.dm_limit_input = ctk.CTkEntry(self.acc_frame, placeholder_text="DM daily limit (np. 15)", width=350)
        self.dm_limit_input.grid(row=3, column=0, padx=10, pady=5)
        self.dm_limit_input.insert(0, "15")
        self.join_limit_input = ctk.CTkEntry(self.acc_frame, placeholder_text="Join daily limit (np. 5)", width=350)
        self.join_limit_input.grid(row=4, column=0, padx=10, pady=5)
        self.join_limit_input.insert(0, "5")
        self.add_acc_btn = ctk.CTkButton(self.acc_frame, text="Add Account", command=self.add_account)
        self.add_acc_btn.grid(row=1, column=1, rowspan=4, padx=10, pady=5, sticky="ns")
        self.remove_account_input = ctk.CTkEntry(self.acc_frame, placeholder_text="Account ID to remove", width=350)
        self.remove_account_input.grid(row=5, column=0, padx=10, pady=5)
        self.remove_account_btn = ctk.CTkButton(self.acc_frame, text="Remove Account", fg_color="#e74c3c", command=self.remove_account_by_id)
        self.remove_account_btn.grid(row=5, column=1, padx=10, pady=5)
        self.export_banlist_plaintext_toggle = ctk.CTkCheckBox(
            self.acc_frame,
            text="Export banlist tokens in plaintext (unsafe)",
            variable=self.export_banned_tokens_plaintext_var,
            command=self.on_export_plaintext_toggle,
        )
        self.export_banlist_plaintext_toggle.grid(row=6, column=0, columnspan=2, padx=10, pady=(5, 0), sticky="w")

        self.acc_overview_frame = ctk.CTkFrame(parent)
        self.acc_overview_frame.pack(fill="x", pady=10)
        ctk.CTkLabel(self.acc_overview_frame, text="Account Counters & Status", font=ctk.CTkFont(size=16, weight="bold")).grid(row=0, column=0, columnspan=2, pady=10)
        self.acc_overview_box = ctk.CTkTextbox(self.acc_overview_frame, height=120)
        self.acc_overview_box.grid(row=1, column=0, padx=10, pady=5, sticky="ew")
        self.acc_overview_frame.grid_columnconfigure(0, weight=1)
        self.acc_refresh_btn = ctk.CTkButton(self.acc_overview_frame, text="Refresh Accounts", command=self.refresh_accounts_overview)
        self.acc_refresh_btn.grid(row=1, column=1, padx=10, pady=5)
        self.acc_reset_btn = ctk.CTkButton(self.acc_overview_frame, text="Reset Counters", fg_color="#f39c12", hover_color="#d35400", command=self.reset_account_counters)
        self.acc_reset_btn.grid(row=2, column=1, padx=10, pady=5)

        self.module_frame = ctk.CTkFrame(parent)
        self.module_frame.pack(fill="x", pady=10)
        ctk.CTkLabel(self.module_frame, text="Module Switches", font=ctk.CTkFont(size=16, weight="bold")).grid(row=0, column=0, columnspan=2, pady=10)
        self.dm_toggle = ctk.CTkCheckBox(self.module_frame, text="DM Module", variable=self.module_vars["dm"], command=self.on_module_toggle)
        self.dm_toggle.grid(row=1, column=0, padx=10, pady=5, sticky="w")
        self.joiner_toggle = ctk.CTkCheckBox(self.module_frame, text="Joiner Module", variable=self.module_vars["joiner"], command=self.on_module_toggle)
        self.joiner_toggle.grid(row=1, column=1, padx=10, pady=5, sticky="w")
        self.scraper_toggle = ctk.CTkCheckBox(self.module_frame, text="Scraper Module", variable=self.module_vars["scraper"], command=self.on_module_toggle)
        self.scraper_toggle.grid(row=2, column=0, padx=10, pady=5, sticky="w")
        self.status_toggle = ctk.CTkCheckBox(self.module_frame, text="Status Module", variable=self.module_vars["status"], command=self.on_module_toggle)
        self.status_toggle.grid(row=2, column=1, padx=10, pady=5, sticky="w")
        self.captcha_toggle = ctk.CTkCheckBox(self.module_frame, text="Captcha Module", variable=self.module_vars["captcha"], command=self.on_module_toggle)
        self.captcha_toggle.grid(row=3, column=0, padx=10, pady=5, sticky="w")

        self.msg_frame = ctk.CTkFrame(parent)
        self.msg_frame.pack(fill="x", pady=10)
        ctk.CTkLabel(self.msg_frame, text="Message Templates", font=ctk.CTkFont(size=16, weight="bold")).pack(pady=5)
        ctk.CTkLabel(
            self.msg_frame,
            text="Szablony oddzielaj liniÄ…: --- | Tokeny: [[tag]], [[emoji]], [[num]], [[num:1-99]] | Spintax: {a|b}",
            font=ctk.CTkFont(size=12),
            text_color="#b0b0b0",
        ).pack(pady=(0, 5))
        self.msg_input = ctk.CTkTextbox(self.msg_frame, height=100)
        self.msg_input.pack(fill="x", padx=20, pady=10)
        self.friend_request_toggle = ctk.CTkCheckBox(
            self.msg_frame,
            text="WyĹ›lij zaproszenie do znajomych przed DM",
            variable=self.friend_request_var,
        )
        self.friend_request_toggle.pack(anchor="w", padx=20, pady=(0, 10))

        self.delay_frame = ctk.CTkFrame(parent)
        self.delay_frame.pack(fill="x", pady=10)
        ctk.CTkLabel(self.delay_frame, text="Delay Settings", font=ctk.CTkFont(size=16, weight="bold")).grid(row=0, column=0, columnspan=4, pady=10)

        dm_delay_min = int(self._get_setting_number("dm_delay_min", 5))
        dm_delay_max = int(self._get_setting_number("dm_delay_max", 10))
        join_delay_min = int(self._get_setting_number("join_delay_min", 10))
        join_delay_max = int(self._get_setting_number("join_delay_max", 30))
        friend_delay_min = int(self._get_setting_number("friend_delay_min", 2))
        friend_delay_max = int(self._get_setting_number("friend_delay_max", 5))
        status_delay_min = self._get_setting_number("status_delay_min_hours", 3.0)
        status_delay_max = self._get_setting_number("status_delay_max_hours", 3.0)
        account_min_interval = int(self._get_setting_number("account_min_interval", 0))
        target_min_interval = int(self._get_setting_number("target_min_interval", 0))

        ctk.CTkLabel(self.delay_frame, text="DM delay (s) min/max").grid(row=1, column=0, padx=10, pady=5, sticky="w")
        self.dm_delay_min_input = ctk.CTkEntry(self.delay_frame, width=120)
        self.dm_delay_min_input.grid(row=1, column=1, padx=10, pady=5, sticky="w")
        self.dm_delay_min_input.insert(0, str(dm_delay_min))
        self.dm_delay_max_input = ctk.CTkEntry(self.delay_frame, width=120)
        self.dm_delay_max_input.grid(row=1, column=2, padx=10, pady=5, sticky="w")
        self.dm_delay_max_input.insert(0, str(dm_delay_max))

        ctk.CTkLabel(self.delay_frame, text="Join delay (s) min/max").grid(row=2, column=0, padx=10, pady=5, sticky="w")
        self.join_delay_min_input = ctk.CTkEntry(self.delay_frame, width=120)
        self.join_delay_min_input.grid(row=2, column=1, padx=10, pady=5, sticky="w")
        self.join_delay_min_input.insert(0, str(join_delay_min))
        self.join_delay_max_input = ctk.CTkEntry(self.delay_frame, width=120)
        self.join_delay_max_input.grid(row=2, column=2, padx=10, pady=5, sticky="w")
        self.join_delay_max_input.insert(0, str(join_delay_max))

        ctk.CTkLabel(self.delay_frame, text="Friend request delay (s) min/max").grid(row=3, column=0, padx=10, pady=5, sticky="w")
        self.friend_delay_min_input = ctk.CTkEntry(self.delay_frame, width=120)
        self.friend_delay_min_input.grid(row=3, column=1, padx=10, pady=5, sticky="w")
        self.friend_delay_min_input.insert(0, str(friend_delay_min))
        self.friend_delay_max_input = ctk.CTkEntry(self.delay_frame, width=120)
        self.friend_delay_max_input.grid(row=3, column=2, padx=10, pady=5, sticky="w")
        self.friend_delay_max_input.insert(0, str(friend_delay_max))

        ctk.CTkLabel(self.delay_frame, text="Status delay (h) min/max").grid(row=4, column=0, padx=10, pady=5, sticky="w")
        self.status_delay_min_input = ctk.CTkEntry(self.delay_frame, width=120)
        self.status_delay_min_input.grid(row=4, column=1, padx=10, pady=5, sticky="w")
        self.status_delay_min_input.insert(0, str(status_delay_min))
        self.status_delay_max_input = ctk.CTkEntry(self.delay_frame, width=120)
        self.status_delay_max_input.grid(row=4, column=2, padx=10, pady=5, sticky="w")
        self.status_delay_max_input.insert(0, str(status_delay_max))

        ctk.CTkLabel(self.delay_frame, text="Account min interval (s)").grid(row=5, column=0, padx=10, pady=5, sticky="w")
        self.account_min_interval_input = ctk.CTkEntry(self.delay_frame, width=120)
        self.account_min_interval_input.grid(row=5, column=1, padx=10, pady=5, sticky="w")
        self.account_min_interval_input.insert(0, str(account_min_interval))

        ctk.CTkLabel(self.delay_frame, text="Target min interval (s)").grid(row=6, column=0, padx=10, pady=5, sticky="w")
        self.target_min_interval_input = ctk.CTkEntry(self.delay_frame, width=120)
        self.target_min_interval_input.grid(row=6, column=1, padx=10, pady=5, sticky="w")
        self.target_min_interval_input.insert(0, str(target_min_interval))

        self.delay_save_btn = ctk.CTkButton(self.delay_frame, text="Save Delays", command=self.save_delay_settings)
        self.delay_save_btn.grid(row=7, column=0, padx=10, pady=10, sticky="w")

        # 2. SEKCJA JOINERA (NOWOĹšÄ†)
        self.joiner_frame = ctk.CTkFrame(parent)
        self.joiner_frame.pack(fill="x", pady=10)
        ctk.CTkLabel(self.joiner_frame, text="Server Joiner (Mass Join)", font=ctk.CTkFont(size=16, weight="bold")).grid(row=0, column=0, columnspan=2, pady=10)
        self.invite_input = ctk.CTkTextbox(self.joiner_frame, height=80, width=350)
        self.invite_input.grid(row=1, column=0, padx=10, pady=5)
        self.invite_input.insert("1.0", "Invite link/code per line (e.g. discord.gg/xyz)\n")
        self.join_btn = ctk.CTkButton(self.joiner_frame, text="Join Server", fg_color="#f39c12", hover_color="#d35400", command=self.start_joining)
        self.join_btn.grid(row=1, column=1, padx=10, pady=5)

        # 3. SEKCJA CAPTCHA
        self.captcha_frame = ctk.CTkFrame(parent)
        self.captcha_frame.pack(fill="x", pady=10)
        ctk.CTkLabel(self.captcha_frame, text="Captcha Solver (CapSolver / 2Captcha / Anti-Captcha)", font=ctk.CTkFont(size=16, weight="bold")).grid(row=0, column=0, columnspan=3, pady=10)
        self.captcha_provider_labels = {
            "CapSolver": "capsolver",
            "2Captcha": "2captcha",
            "Anti-Captcha": "anticaptcha",
        }
        self.captcha_provider_display = {value: key for key, value in self.captcha_provider_labels.items()}
        self.captcha_provider_var = ctk.StringVar(value=self.captcha_provider_display["capsolver"])
        self.captcha_provider = ctk.CTkOptionMenu(
            self.captcha_frame,
            values=list(self.captcha_provider_labels.keys()),
            variable=self.captcha_provider_var,
            command=self.on_captcha_provider_change,
        )
        self.captcha_provider.grid(row=1, column=0, padx=10, pady=5)
        self.captcha_key_input = ctk.CTkEntry(self.captcha_frame, placeholder_text="API Key", width=350)
        self.captcha_key_input.grid(row=1, column=1, padx=10, pady=5)
        self.captcha_save_btn = ctk.CTkButton(self.captcha_frame, text="Save", command=self.save_captcha_settings)
        self.captcha_save_btn.grid(row=1, column=2, padx=10, pady=5)
        self.captcha_test_btn = ctk.CTkButton(self.captcha_frame, text="Test API", fg_color="#16a085", command=self.test_captcha_settings)
        self.captcha_test_btn.grid(row=2, column=1, padx=10, pady=5)
        self._load_captcha_settings()

        self.version_frame = ctk.CTkFrame(parent)
        self.version_frame.pack(fill="x", pady=10)
        ctk.CTkLabel(self.version_frame, text="Version Checker", font=ctk.CTkFont(size=16, weight="bold")).grid(row=0, column=0, columnspan=2, pady=10)
        ctk.CTkLabel(self.version_frame, text=f"Current version: {APP_VERSION}").grid(row=1, column=0, columnspan=2, pady=(0, 5))
        ctk.CTkLabel(self.version_frame, text="Latest version endpoint (JSON)").grid(row=2, column=0, padx=10, pady=5, sticky="w")
        self.version_endpoint_input = ctk.CTkEntry(self.version_frame, width=450, placeholder_text=DEFAULT_VERSION_ENDPOINT)
        self.version_endpoint_input.grid(row=2, column=1, padx=10, pady=5, sticky="w")
        stored_endpoint = self.db.get_setting("version_endpoint", "")
        if stored_endpoint:
            self.version_endpoint_input.insert(0, stored_endpoint)
        self.version_save_btn = ctk.CTkButton(self.version_frame, text="Save Endpoint", command=self.save_version_settings)
        self.version_save_btn.grid(row=3, column=0, padx=10, pady=10, sticky="w")
        self.version_check_btn = ctk.CTkButton(self.version_frame, text="Check Now", fg_color="#3498db", command=self.check_for_updates)
        self.version_check_btn.grid(row=3, column=1, padx=10, pady=10, sticky="w")
        self.version_status_label = ctk.CTkLabel(self.version_frame, textvariable=self.version_status_var, text_color="#8a8a8a")
        self.version_status_label.grid(row=4, column=0, padx=10, pady=(0, 10), sticky="w")
        self.manual_update_btn = ctk.CTkButton(
            self.version_frame,
            text="Manual Update",
            fg_color="#1abc9c",
            command=self.manual_update_with_prompt,
            state="disabled",
        )
        self.manual_update_btn.grid(row=4, column=1, padx=10, pady=(0, 10), sticky="w")
        self.update_download_btn = ctk.CTkButton(
            self.version_frame,
            text="Download & Install",
            fg_color="#27ae60",
            command=self.download_update,
            state="disabled",
        )
        self.update_download_btn.grid(row=5, column=0, padx=10, pady=(0, 10), sticky="w")
        self.update_status_label = ctk.CTkLabel(self.version_frame, textvariable=self.update_status_var, text_color="#8a8a8a")
        self.update_status_label.grid(row=5, column=1, padx=10, pady=(0, 10), sticky="w")

        self.target_frame = ctk.CTkFrame(parent)
        self.target_frame.pack(fill="x", pady=10)
        ctk.CTkLabel(self.target_frame, text="Target List Management", font=ctk.CTkFont(size=16, weight="bold")).grid(row=0, column=0, columnspan=2, pady=10)
        self.target_input = ctk.CTkTextbox(self.target_frame, height=100, width=350)
        self.target_input.grid(row=1, column=0, padx=10, pady=5, sticky="ew")
        self.target_frame.grid_columnconfigure(0, weight=1)
        self.target_add_btn = ctk.CTkButton(self.target_frame, text="Add Targets", command=self.add_targets_from_input)
        self.target_add_btn.grid(row=1, column=1, padx=10, pady=5)
        self.target_import_btn = ctk.CTkButton(self.target_frame, text="Import z pliku (.txt)", command=self.import_targets_from_file)
        self.target_import_btn.grid(row=1, column=2, padx=10, pady=5)
        self.target_clear_btn = ctk.CTkButton(self.target_frame, text="Clear List", fg_color="#e67e22", command=self.clear_targets)
        self.target_clear_btn.grid(row=2, column=1, padx=10, pady=5)
        self.target_refresh_btn = ctk.CTkButton(self.target_frame, text="Refresh List", command=self.refresh_targets_overview)
        self.target_refresh_btn.grid(row=3, column=1, padx=10, pady=5)
        self.target_summary_label = ctk.CTkLabel(self.target_frame, text="Targets: 0", anchor="w")
        self.target_summary_label.grid(row=2, column=0, padx=10, pady=5, sticky="w")
        self.target_overview_box = ctk.CTkTextbox(self.target_frame, height=100)
        self.target_overview_box.grid(row=3, column=0, padx=10, pady=5, sticky="ew")

        self.banlist_frame = ctk.CTkFrame(parent)
        self.banlist_frame.pack(fill="x", pady=10)
        ctk.CTkLabel(
            self.banlist_frame,
            text="Banlist (Banned/Dead Tokens)",
            font=ctk.CTkFont(size=16, weight="bold"),
        ).grid(row=0, column=0, columnspan=2, pady=10)
        self.banlist_summary_label = ctk.CTkLabel(self.banlist_frame, text="Banlist: 0", anchor="w")
        self.banlist_summary_label.grid(row=1, column=0, padx=10, pady=5, sticky="w")
        self.banlist_refresh_btn = ctk.CTkButton(
            self.banlist_frame,
            text="Refresh Banlist",
            command=self.refresh_banlist_overview,
        )
        self.banlist_refresh_btn.grid(row=1, column=1, padx=10, pady=5, sticky="e")
        self.banlist_box = ctk.CTkTextbox(self.banlist_frame, height=140)
        self.banlist_box.grid(row=2, column=0, columnspan=2, padx=10, pady=5, sticky="ew")
        self.banlist_frame.grid_columnconfigure(0, weight=1)

        # 4. SEKCJA STATUSU
        self.status_frame = ctk.CTkFrame(parent)
        self.status_frame.pack(fill="x", pady=10)
        ctk.CTkLabel(self.status_frame, text="Status & Presence", font=ctk.CTkFont(size=16, weight="bold")).grid(row=0, column=0, columnspan=2, pady=10)
        self.status_text_input = ctk.CTkEntry(self.status_frame, placeholder_text="Custom Status", width=350)
        self.status_text_input.grid(row=1, column=0, padx=10, pady=5)
        self.status_text_input.insert(0, "Playing Metin2")
        self.status_type_var = ctk.StringVar(value="online")
        self.status_dropdown = ctk.CTkOptionMenu(self.status_frame, values=["online", "idle", "dnd", "invisible"], variable=self.status_type_var)
        self.status_dropdown.grid(row=1, column=1, padx=10, pady=5)
        self.update_status_btn = ctk.CTkButton(self.status_frame, text="Start Auto Status", fg_color="#9b59b6", command=self.start_status_update)
        self.update_status_btn.grid(row=2, column=0, pady=10, sticky="w")
        self.stop_status_btn = ctk.CTkButton(self.status_frame, text="Stop Auto Status", fg_color="#c0392b", command=self.stop_status_update)
        self.stop_status_btn.grid(row=2, column=1, pady=10, sticky="e")

        # 5. SEKCJA SCRAPERA
        self.scrape_frame = ctk.CTkFrame(parent)
        self.scrape_frame.pack(fill="x", pady=10)
        ctk.CTkLabel(self.scrape_frame, text="Scraping Tools", font=ctk.CTkFont(size=16, weight="bold")).grid(row=0, column=0, columnspan=2, pady=10)
        self.scrape_channel_input = ctk.CTkEntry(self.scrape_frame, placeholder_text="Channel ID", width=220)
        self.scrape_channel_input.grid(row=1, column=0, padx=10, pady=5, sticky="w")
        self.scrape_range_input = ctk.CTkEntry(self.scrape_frame, placeholder_text="Range (limit)", width=140)
        self.scrape_range_input.grid(row=1, column=1, padx=10, pady=5, sticky="w")
        self.scrape_btn = ctk.CTkButton(self.scrape_frame, text="Scrape Users", command=self.start_scraping)
        self.scrape_btn.grid(row=1, column=2, padx=10, pady=5)
        self.scrape_guild_input = ctk.CTkEntry(self.scrape_frame, placeholder_text="Guild ID", width=350)
        self.scrape_guild_input.grid(row=2, column=0, padx=10, pady=5, columnspan=2, sticky="w")
        self.scrape_guild_btn = ctk.CTkButton(
            self.scrape_frame,
            text="Scrape Guild Members",
            fg_color="#16a085",
            command=self.start_guild_scraping,
        )
        self.scrape_guild_btn.grid(row=2, column=2, padx=10, pady=5, sticky="w")
        self.refresh_accounts_overview()
        self.refresh_targets_overview()
        self.refresh_banlist_overview()
        self.apply_module_states()
        self.refresh_workflow_status()

    def on_captcha_provider_change(self, _value=None):
        self._refresh_captcha_key()

    def _normalize_captcha_provider(self, provider: str) -> str:
        normalized = (provider or "").strip().lower()
        if normalized in {"anti-captcha", "anti captcha", "anticaptcha"}:
            return "anticaptcha"
        if normalized in {"capsolver", "2captcha"}:
            return normalized
        return provider

    def _get_captcha_provider_key(self) -> str:
        provider = self.captcha_provider_var.get()
        provider = self.captcha_provider_labels.get(provider, provider)
        return self._normalize_captcha_provider(provider)

    def _load_captcha_settings(self):
        provider = self._normalize_captcha_provider(self.captcha_solver.get_provider())
        self.captcha_provider_var.set(self.captcha_provider_display.get(provider, provider))
        self._refresh_captcha_key()

    def _refresh_captcha_key(self):
        provider = self._get_captcha_provider_key()
        stored_key = self.db.get_setting(f"{provider}_api_key", "")
        if not stored_key and provider == "anticaptcha":
            stored_key = self.db.get_setting("anti-captcha_api_key", "")
            if stored_key:
                self.db.set_setting("anticaptcha_api_key", stored_key)
        self.captcha_key_input.delete(0, "end")
        if stored_key:
            self.captcha_key_input.insert(0, stored_key)

    def save_captcha_settings(self):
        provider = self._get_captcha_provider_key()
        api_key = self.captcha_key_input.get().strip()
        self.db.set_setting("captcha_provider", provider)
        self.db.set_setting(f"{provider}_api_key", api_key)
        self.add_log(f"[Captcha] Zapisano ustawienia dla {provider}.")

    def test_captcha_settings(self):
        provider = self._get_captcha_provider_key()
        api_key = self.captcha_key_input.get().strip()
        self.add_log(f"[Captcha] Sprawdzam API {provider}...")
        thread = threading.Thread(target=self._run_captcha_check, args=(provider, api_key))
        thread.daemon = True
        thread.start()

    def _run_captcha_check(self, provider, api_key):
        ok, msg = self.captcha_solver.check_balance(provider, api_key)
        if ok:
            self.add_log(f"[Captcha] OK ({provider}) - {msg}")
        else:
            self.add_log(f"[Captcha] BĹ‚Ä…d ({provider}) - {msg}")

if __name__ == "__main__":
    app = MassDMApp()
    app.mainloop()















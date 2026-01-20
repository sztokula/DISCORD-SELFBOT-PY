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
from joiner import DiscordJoiner # Import new module
from token_manager import TokenManager
from updater import UpdateManager, UpdateError
from metrics import HealthMetrics
from profile_updater import ProfileUpdater
import httpx

ctk.set_appearance_mode("Dark")
ctk.set_default_color_theme("blue")

APP_VERSION = "1.0"
DEFAULT_VERSION_ENDPOINT = "https://updates.example.com/massdm/version.json"
TRUSTED_UPDATE_HOSTS = {"updates.example.com"}

class MassDMApp(ctk.CTk):
    def __init__(self):
        super().__init__()

        self.log_queue = queue.Queue()
        self.db = DatabaseManager(log_callback=self.add_log)
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
        
        self.metrics = HealthMetrics()
        self.captcha_solver = CaptchaSolver(self.db, self.add_log)
        self.worker = DiscordWorker(self.db, self.add_log, self.metrics, self.captcha_solver)
        self.scraper = DiscordScraper(self.db, self.add_log, self.metrics)
        self.status_changer = StatusChanger(self.db, self.add_log, self.metrics)
        self.joiner = DiscordJoiner(self.db, self.add_log, self.captcha_solver, self.metrics) # Inicjalizacja
        self.token_manager = TokenManager(self.db, self.add_log, self.metrics)
        self.profile_updater = ProfileUpdater(self.db, self.add_log, self.metrics)
        self.log_entries = []
        self.error_entries = []
        self.max_log_entries = 2000
        self.max_error_entries = 2000
        self.unverified_retry_delay_seconds = 60
        self.max_unverified_retries = 3
        self.log_filter_var = ctk.StringVar()
        self.error_filter_var = ctk.StringVar()
        self.log_level_var = ctk.StringVar(value="All")
        self._proxy_check_cache = {}
        self._proxy_check_ttl_seconds = 600
        self._proxy_check_lock = threading.Lock()
        self.logs_dir = Path("logs")
        self.logs_dir.mkdir(parents=True, exist_ok=True)
        self.log_file_path = self.logs_dir / f"app_{datetime.now().strftime('%Y%m%d')}.log"
        self.error_log_file_path = self.logs_dir / f"errors_{datetime.now().strftime('%Y%m%d')}.log"
        self.banlist_path = Path("banned_dead_tokens.txt")

        self.title(f"Mass-DM Farm Tool Pro v{APP_VERSION}")
        self.geometry("1100x1000") # Increased height for the new section.

        self.grid_columnconfigure(1, weight=1)
        self.grid_rowconfigure(0, weight=1)
        self.settings_window = None
        self._settings_scroll_fix_bound = False
        self._settings_scroll_fix_bind_id = None
        self._settings_scrollbar_original = None
        self._settings_scrollbar_widget = None
        self._settings_refresh_job = None
        self._template_editing = False
        self._template_max_chars = 2000
        self._template_save_job = None
        self._invalid_input_color = "#e74c3c"
        self._input_border_defaults = {}
        self.version_status_var = ctk.StringVar(value="Last check: --")
        self.update_status_var = ctk.StringVar(value="Update: --")
        self.update_info = None
        self.manual_update_requested = False
        self.notification_var = ctk.StringVar(value="Notifications: --")

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

        # 0. NOTIFICATION SECTION
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
        self.health_alert_label = ctk.CTkLabel(self.health_frame, text="Alerts: --", anchor="w", text_color="#8a8a8a")
        self.health_alert_label.grid(row=2, column=0, columnspan=4, padx=10, pady=(0, 10), sticky="w")

        self.workflow_frame = ctk.CTkFrame(self.main_container)
        self.workflow_frame.pack(fill="x", pady=(0, 10))
        for col in range(2):
            self.workflow_frame.grid_columnconfigure(col, weight=1)
        ctk.CTkLabel(self.workflow_frame, text="Start Steps", font=ctk.CTkFont(size=16, weight="bold")).grid(
            row=0, column=0, columnspan=2, pady=10
        )
        self.step_accounts_label = ctk.CTkLabel(self.workflow_frame, text="1. Accounts: --", anchor="w")
        self.step_accounts_label.grid(row=1, column=0, padx=10, pady=2, sticky="w")
        self.step_proxies_label = ctk.CTkLabel(self.workflow_frame, text="2. Proxy: --", anchor="w")
        self.step_proxies_label.grid(row=2, column=0, padx=10, pady=2, sticky="w")
        self.step_invites_label = ctk.CTkLabel(self.workflow_frame, text="3. Servers: --", anchor="w")
        self.step_invites_label.grid(row=3, column=0, padx=10, pady=2, sticky="w")
        self.step_templates_label = ctk.CTkLabel(self.workflow_frame, text="4. Templates: --", anchor="w")
        self.step_templates_label.grid(row=4, column=0, padx=10, pady=2, sticky="w")
        self.step_settings_label = ctk.CTkLabel(self.workflow_frame, text="5. Settings: --", anchor="w")
        self.step_settings_label.grid(row=5, column=0, padx=10, pady=2, sticky="w")

        self.workflow_refresh_btn = ctk.CTkButton(self.workflow_frame, text="Refresh", command=self.refresh_workflow_status)
        self.workflow_refresh_btn.grid(row=1, column=1, padx=10, pady=5, sticky="e")
        self.workflow_next_btn = ctk.CTkButton(self.workflow_frame, text="Next", command=self.advance_workflow)
        self.workflow_next_btn.grid(row=2, column=1, padx=10, pady=5, sticky="e")
        self.workflow_action_btn = ctk.CTkButton(
            self.workflow_frame,
            text="JOIN",
            fg_color="#f39c12",
            hover_color="#d35400",
            command=self._handle_join_action,
        )
        self.workflow_action_btn.grid(row=2, column=1, padx=10, pady=5, sticky="e")
        self.workflow_action_btn.grid_remove()

        self._set_workflow_stage("setup")

        # 1. MESSAGE SECTION (moved to settings)
        self.friend_request_var = ctk.BooleanVar(value=self._get_setting_bool("use_friend_request", False))
        self.dry_run_var = ctk.BooleanVar(value=self._get_setting_bool("dry_run", False))
        self.auto_accept_rules_var = ctk.BooleanVar(value=self._get_setting_bool("auto_accept_rules", True))
        self.auto_onboarding_var = ctk.BooleanVar(value=self._get_setting_bool("auto_onboarding", True))
        self.require_proxy_var = ctk.BooleanVar(value=self._get_setting_bool("require_proxy", True))
        self.profile_change_name_var = ctk.BooleanVar(value=self._get_setting_bool("profile_change_name", False))
        self.profile_change_avatar_var = ctk.BooleanVar(value=self._get_setting_bool("profile_change_avatar", False))
        self.profile_append_suffix_var = ctk.BooleanVar(value=self._get_setting_bool("profile_append_suffix", True))
        self.settings_loaded = False
        self.workflow_stage = "setup"

        # 2. LOGS SECTION
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
        self.account_status_colors = {
            "active": "#2ecc71",
            "unverified": "#f39c12",
            "banned": "#e74c3c",
            "dead": "#e74c3c",
            "banned/dead": "#e74c3c",
        }

    def add_log(self, message):
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        level = self._get_log_level(message)
        self.log_queue.put({"timestamp": timestamp, "message": message, "level": level})

    def log_error(self, message):
        self.add_log(f"[Error] {message}")

    def log_warning(self, message):
        self.add_log(f"[Warn] {message}")

    def _register_input_widget(self, widget):
        if not widget or widget in self._input_border_defaults:
            return
        try:
            self._input_border_defaults[widget] = widget.cget("border_color")
        except Exception:
            self._input_border_defaults[widget] = None

    def _set_input_valid(self, widget, is_valid):
        if not widget:
            return
        try:
            if not widget.winfo_exists():
                return
        except Exception:
            return
        if widget not in self._input_border_defaults:
            self._register_input_widget(widget)
        try:
            if is_valid:
                default = self._input_border_defaults.get(widget)
                if default is not None:
                    widget.configure(border_color=default)
            else:
                widget.configure(border_color=self._invalid_input_color)
        except Exception:
            return

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

    def _delay_values_in_hours(self):
        return self._get_setting_bool("delay_units_hours", False)

    def _convert_delay_value(self, value, assume_seconds=False):
        if value is None:
            return None
        try:
            value = float(value)
        except (TypeError, ValueError):
            return None
        if self._delay_values_in_hours():
            return value
        if assume_seconds:
            return value / 3600.0
        return value

    def _seconds_to_hours(self, seconds):
        return max(0.0, float(seconds)) / 3600.0

    def _hours_to_seconds(self, hours):
        return max(0.0, float(hours)) * 3600.0

    def _format_hours_value(self, value):
        try:
            value = float(value)
        except (TypeError, ValueError):
            return ""
        text = f"{value:.4f}".rstrip("0").rstrip(".")
        return text or "0"

    def _is_proxy_required(self):
        if hasattr(self, "require_proxy_var"):
            return self.require_proxy_var.get()
        return self._get_setting_bool("require_proxy", True)

    def _check_proxy_alive(self, proxy):
        if not proxy:
            return False, "Proxy is empty."
        with self._proxy_check_lock:
            cached = self._proxy_check_cache.get(proxy)
        now = datetime.now().timestamp()
        if cached and (now - cached["ts"]) < self._proxy_check_ttl_seconds:
            return cached["ok"], cached["err"]
        try:
            with httpx.Client(
                proxies={"all://": proxy},
                timeout=httpx.Timeout(8.0),
                headers={"User-Agent": "Mozilla/5.0"},
            ) as client:
                resp = client.get("https://discord.com/api/v9/experiments")
            if resp.status_code == 407:
                ok, err = False, "Proxy auth failed (407)."
            else:
                ok, err = True, None
        except Exception as exc:
            ok, err = False, f"Proxy error: {exc}"
        with self._proxy_check_lock:
            self._proxy_check_cache[proxy] = {"ok": ok, "err": err, "ts": now}
        return ok, err

    def _is_proxy_format_valid(self, proxy):
        if not proxy:
            return False
        parsed = urlparse(proxy)
        if parsed.scheme and parsed.hostname and parsed.port:
            return parsed.scheme in {"http", "https", "socks5"}
        parsed = urlparse(f"http://{proxy}")
        return bool(parsed.hostname and parsed.port)

    def _restore_proxy_verified_accounts(self):
        if not self._is_proxy_required():
            return 0
        restored = 0
        accounts = self.db.get_accounts_overview()
        for acc_id, status, proxy, *_rest in accounts:
            status_value = (status or "").strip().casefold()
            if status_value != "unverified":
                continue
            if not proxy or not self._is_proxy_format_valid(proxy):
                continue
            ok, _err = self._check_proxy_alive(proxy)
            if ok:
                self.db.update_account_status(acc_id, "Active")
                restored += 1
        if restored:
            self.add_log(f"[Proxy] Restored {restored} account(s) to Active.")
        return restored

    def _ensure_account_proxies(self, accounts, context_label):
        if not self._is_proxy_required():
            return accounts
        restored = self._restore_proxy_verified_accounts()
        if restored:
            accounts = self.db.get_active_accounts("discord")
        if not accounts:
            self.log_error(f"[Proxy] No accounts available for {context_label}.")
            return []
        valid_accounts = []
        failures = []
        for acc in accounts:
            acc_id, _, _, proxy, _, _, _, _, _, _, _ = acc
            if not proxy:
                failures.append((acc_id, "missing proxy"))
                continue
            if not self._is_proxy_format_valid(proxy):
                failures.append((acc_id, "invalid proxy format"))
                continue
            ok, err = self._check_proxy_alive(proxy)
            if not ok:
                failures.append((acc_id, err))
                continue
            valid_accounts.append(acc)
        if failures:
            self.log_warning(
                f"[Proxy] {context_label}: skipped {len(failures)} account(s) with bad proxy."
            )
            preview = "; ".join(f"{acc_id}: {reason}" for acc_id, reason in failures[:10])
            suffix = "..." if len(failures) > 10 else ""
            self.log_warning(f"[Proxy] {context_label} failures: {preview}{suffix}")
            for acc_id, _reason in failures:
                self.db.update_account_status(acc_id, "Unverified")
        if not valid_accounts:
            self.log_error(f"[Proxy] {context_label}: no accounts with working proxies.")
        return valid_accounts

    def _ensure_scraper_proxy(self, proxy, context_label):
        if not self._is_proxy_required():
            return True
        if not proxy:
            self.log_error(f"[Proxy] Scraper proxy required for {context_label}.")
            return False
        if not self.validate_proxy(proxy):
            self.log_error(f"[Proxy] Invalid scraper proxy format ({context_label}).")
            return False
        ok, err = self._check_proxy_alive(proxy)
        if not ok:
            self.log_error(f"[Proxy] Scraper proxy error ({context_label}): {err}")
            return False
        return True

    def _parse_delay_range(self, min_input, max_input, label, cast_type=int, min_value=0.0):
        min_raw = min_input.get().strip()
        max_raw = max_input.get().strip()
        try:
            min_val = cast_type(min_raw)
        except (TypeError, ValueError):
            self.log_error(f"{label} (min) must be a number.")
            self._set_input_valid(min_input, False)
            return None
        try:
            max_val = cast_type(max_raw)
        except (TypeError, ValueError):
            self.log_error(f"{label} (max) must be a number.")
            self._set_input_valid(max_input, False)
            return None
        if min_val < min_value or max_val < min_value:
            self.log_error(f"{label} must be >= {min_value}.")
            self._set_input_valid(min_input, False)
            self._set_input_valid(max_input, False)
            return None
        if min_val > max_val:
            self.log_error(f"{label} min cannot be greater than max.")
            self._set_input_valid(min_input, False)
            self._set_input_valid(max_input, False)
            return None
        self._set_input_valid(min_input, True)
        self._set_input_valid(max_input, True)
        return min_val, max_val

    def _parse_min_value(self, input_widget, label, cast_type=int, min_value=0.0):
        raw = input_widget.get().strip()
        try:
            value = cast_type(raw)
        except (TypeError, ValueError):
            self.log_error(f"{label} must be a number.")
            self._set_input_valid(input_widget, False)
            return None
        if value < min_value:
            self.log_error(f"{label} must be >= {min_value}.")
            self._set_input_valid(input_widget, False)
            return None
        self._set_input_valid(input_widget, True)
        return value

    def _parse_min_value_from_settings(self, key, label, cast_type=int, min_value=0.0, assume_seconds=False):
        value = self._get_setting_number(key, None)
        if value is None:
            self.log_error(f"{label} is not set. Open settings.")
            return None
        value = self._convert_delay_value(value, assume_seconds=assume_seconds)
        if value is None:
            self.log_error(f"{label} has an invalid format.")
            return None
        try:
            value = cast_type(value)
        except (TypeError, ValueError):
            self.log_error(f"{label} has an invalid format.")
            return None
        if value < min_value:
            self.log_error(f"{label} must be >= {min_value}.")
            return None
        return value

    def _parse_delay_range_from_settings(
        self,
        min_key,
        max_key,
        label,
        cast_type=int,
        min_value=0.0,
        assume_seconds=False,
    ):
        min_val = self._get_setting_number(min_key, None)
        max_val = self._get_setting_number(max_key, None)
        if min_val is None or max_val is None:
            self.log_error(f"{label} is not set. Open settings.")
            return None
        min_val = self._convert_delay_value(min_val, assume_seconds=assume_seconds)
        max_val = self._convert_delay_value(max_val, assume_seconds=assume_seconds)
        if min_val is None or max_val is None:
            self.log_error(f"{label} has an invalid format.")
            return None
        try:
            min_val = cast_type(min_val)
            max_val = cast_type(max_val)
        except (TypeError, ValueError):
            self.log_error(f"{label} has an invalid format.")
            return None
        if min_val < min_value or max_val < min_value:
            self.log_error(f"{label} must be >= {min_value}.")
            return None
        if min_val > max_val:
            self.log_error(f"{label} min cannot be greater than max.")
            return None
        return min_val, max_val

    def _count_templates(self):
        templates = self._get_message_templates()
        return sum(1 for template in templates if template.strip())

    def _deserialize_templates(self, raw):
        if not raw:
            return [""] * 10
        raw_value = str(raw).strip()
        if not raw_value:
            return [""] * 10
        templates = None
        try:
            parsed = json.loads(raw_value)
        except (TypeError, ValueError, json.JSONDecodeError):
            parsed = None
        if isinstance(parsed, list):
            templates = ["" if item is None else str(item) for item in parsed]
        else:
            templates = re.split(r"\n-{3,}\n", raw_value)
        templates = templates or []
        if len(templates) < 10:
            templates.extend([""] * (10 - len(templates)))
        return templates[:10]

    def _get_message_templates(self):
        if hasattr(self, "template_boxes") and self.template_boxes:
            if self.template_boxes[0].winfo_exists():
                templates = []
                for box in self.template_boxes:
                    templates.append(box.get("1.0", "end-1c"))
                return templates
        stored = self.db.get_setting("message_templates", "")
        return self._deserialize_templates(stored)

    def save_message_templates(self):
        templates = self._get_message_templates()
        if not any(template.strip() for template in templates):
            self.db.set_setting("message_templates", None)
            self.add_log("[Templates] Cleared message templates.")
            self._set_template_save_status("Save status: cleared", "#f39c12")
            self.refresh_workflow_status()
            return
        payload = json.dumps(templates, ensure_ascii=True)
        self.db.set_setting("message_templates", payload)
        self.add_log("[Templates] Saved message templates.")
        self._set_template_save_status("Save status: saved", "#2ecc71")
        self.refresh_workflow_status()

    def _schedule_save_message_templates(self):
        if self._template_save_job is not None:
            try:
                self.after_cancel(self._template_save_job)
            except Exception:
                pass
        self._set_template_save_status("Save status: pending", "#f39c12")
        self._template_save_job = self.after(300, self._save_message_templates_async)

    def _save_message_templates_async(self):
        self._template_save_job = None
        self._set_template_save_status("Save status: saving...", "#f39c12")
        templates = self._get_message_templates()
        if not any(template.strip() for template in templates):
            self.db.set_setting("message_templates", None)
            self.after(0, lambda: self.add_log("[Templates] Cleared message templates."))
            self.after(0, lambda: self._set_template_save_status("Save status: cleared", "#f39c12"))
            self.after(0, self.refresh_workflow_status)
            return
        payload = json.dumps(templates, ensure_ascii=True)
        def _worker():
            self.db.set_setting("message_templates", payload)
            self.after(0, lambda: self.add_log("[Templates] Saved message templates."))
            self.after(0, lambda: self._set_template_save_status("Save status: saved", "#2ecc71"))
            self.after(0, self.refresh_workflow_status)
        thread = threading.Thread(target=_worker, daemon=True)
        thread.start()

    def _load_message_templates(self):
        if not hasattr(self, "template_boxes"):
            return
        templates = self._deserialize_templates(self.db.get_setting("message_templates", ""))
        self._template_editing = True
        for box, template in zip(self.template_boxes, templates):
            box.delete("1.0", "end")
            if template:
                box.insert("1.0", template)
            box.edit_modified(False)
        self._template_editing = False
        self._refresh_template_counters()

    def save_scrape_settings(self):
        token = None
        if hasattr(self, "token_input") and self.token_input.winfo_exists():
            token = self.token_input.get().strip()
        channel_id = None
        if hasattr(self, "scrape_channel_input") and self.scrape_channel_input.winfo_exists():
            channel_id = self.scrape_channel_input.get().strip()
        guild_id = None
        if hasattr(self, "scrape_guild_input") and self.scrape_guild_input.winfo_exists():
            guild_id = self.scrape_guild_input.get().strip()
        range_value = None
        if hasattr(self, "scrape_range_input") and self.scrape_range_input.winfo_exists():
            range_value = self.scrape_range_input.get().strip()

        self.db.set_setting("scrape_token", token or None)
        self.db.set_setting("scrape_channel_id", channel_id or None)
        self.db.set_setting("scrape_guild_id", guild_id or None)
        self.db.set_setting("scrape_range", range_value or None)
        scrape_proxy = None
        if hasattr(self, "scrape_proxy_input") and self.scrape_proxy_input.winfo_exists():
            scrape_proxy = self.scrape_proxy_input.get().strip()
        self.db.set_setting("scrape_proxy", scrape_proxy or None)

    def _load_scrape_settings(self):
        if not hasattr(self, "scrape_channel_input"):
            return
        token = self.db.get_setting("scrape_token", "").strip()
        if token and hasattr(self, "token_input") and self.token_input.winfo_exists():
            self.token_input.delete(0, "end")
            self.token_input.insert(0, token)
        channel_id = self.db.get_setting("scrape_channel_id", "").strip()
        if channel_id:
            self.scrape_channel_input.delete(0, "end")
            self.scrape_channel_input.insert(0, channel_id)
        guild_id = self.db.get_setting("scrape_guild_id", "").strip()
        if guild_id:
            self.scrape_guild_input.delete(0, "end")
            self.scrape_guild_input.insert(0, guild_id)
        range_value = self.db.get_setting("scrape_range", "").strip()
        if range_value:
            self.scrape_range_input.delete(0, "end")
            self.scrape_range_input.insert(0, range_value)
        scrape_proxy = self.db.get_setting("scrape_proxy", "").strip()
        if hasattr(self, "scrape_proxy_input") and self.scrape_proxy_input.winfo_exists():
            self.scrape_proxy_input.delete(0, "end")
            if scrape_proxy:
                self.scrape_proxy_input.insert(0, scrape_proxy)

    def save_status_settings(self):
        if not hasattr(self, "status_text_input"):
            return
        status_text = self.status_text_input.get().strip()
        status_type = self.status_type_var.get().strip()
        self.db.set_setting("status_text", status_text or None)
        self.db.set_setting("status_type", status_type or None)

    def save_profile_settings(self):
        if not hasattr(self, "profile_name_input"):
            return
        base_name = self.profile_name_input.get().strip()
        avatar_path = self.profile_avatar_input.get().strip()
        self.db.set_setting("profile_base_name", base_name or None)
        self.db.set_setting("profile_avatar_path", avatar_path or None)
        self._set_setting_bool("profile_change_name", self.profile_change_name_var.get())
        self._set_setting_bool("profile_change_avatar", self.profile_change_avatar_var.get())
        self._set_setting_bool("profile_append_suffix", self.profile_append_suffix_var.get())
        self.add_log("[Profile] Saved profile settings.")

    def _load_profile_settings(self):
        if not hasattr(self, "profile_name_input"):
            return
        base_name = self.db.get_setting("profile_base_name", "").strip()
        avatar_path = self.db.get_setting("profile_avatar_path", "").strip()
        self.profile_name_input.delete(0, "end")
        if base_name:
            self.profile_name_input.insert(0, base_name)
        self.profile_avatar_input.delete(0, "end")
        if avatar_path:
            self.profile_avatar_input.insert(0, avatar_path)

    def browse_avatar_file(self):
        file_path = filedialog.askopenfilename(
            title="Select avatar image",
            filetypes=[
                ("Image files", "*.png;*.jpg;*.jpeg;*.gif;*.webp"),
                ("All files", "*.*"),
            ],
        )
        if not file_path:
            return
        if hasattr(self, "profile_avatar_input"):
            self.profile_avatar_input.delete(0, "end")
            self.profile_avatar_input.insert(0, file_path)
            self._set_input_valid(self.profile_avatar_input, True)

    def apply_profile_settings(self):
        self.save_profile_settings()
        change_name = self.profile_change_name_var.get()
        change_avatar = self.profile_change_avatar_var.get()
        append_suffix = self.profile_append_suffix_var.get()
        base_name = self.db.get_setting("profile_base_name", "").strip()
        avatar_path = self.db.get_setting("profile_avatar_path", "").strip()
        if change_name and not base_name:
            self.log_error("Profile name is empty.")
            self._set_input_valid(self.profile_name_input, False)
            return
        self._set_input_valid(self.profile_name_input, True)
        avatar_data = None
        if change_avatar:
            if not avatar_path:
                self.log_error("Avatar path is empty.")
                self._set_input_valid(self.profile_avatar_input, False)
                return
            avatar_data, err = self.profile_updater.load_avatar_data(avatar_path)
            if err:
                self.log_error(err)
                self._set_input_valid(self.profile_avatar_input, False)
                return
        self._set_input_valid(self.profile_avatar_input, True)
        accounts = self.db.get_active_accounts("discord")
        def _profile_with_proxy_check():
            valid_accounts = self._ensure_account_proxies(accounts, "profile update")
            if not valid_accounts:
                return
            allowed_ids = {acc[0] for acc in valid_accounts}
            self.profile_updater.update_profiles(
                base_name,
                avatar_data,
                change_name,
                change_avatar,
                append_suffix,
                allowed_ids,
            )
        thread = threading.Thread(
            target=_profile_with_proxy_check,
            daemon=True,
        )
        thread.start()

    def _show_template_help(self):
        message = (
            "Templates support tokens and basic Discord formatting.\n\n"
            "Tokens:\n"
            "- [[tag]] or [[tag:promo,info]] selects a random tag.\n"
            "- [[emoji]] or [[emoji:smile,fire]] selects a random emoji.\n"
            "- [[num]] or [[num:1-99]] selects a random number.\n\n"
            "Spintax:\n"
            "- {hello|hi|hey} picks one option.\n\n"
            "Discord formatting examples:\n"
            "- **bold**, *italic*, __underline__, ~~strike~~\n"
            "- `inline code` or ```code block```\n"
            "- > quote, @mentions, #channels, links"
        )
        messagebox.showinfo("Template help", message)

    def _refresh_template_counters(self):
        if not hasattr(self, "template_boxes"):
            return
        for index, box in enumerate(self.template_boxes):
            if not box.winfo_exists():
                continue
            text = box.get("1.0", "end-1c")
            self._update_template_counter(index, len(text))

    def _update_template_counter(self, index, length):
        if not hasattr(self, "template_count_labels"):
            return
        if index >= len(self.template_count_labels):
            return
        label = self.template_count_labels[index]
        if not label.winfo_exists():
            return
        limit = self._template_max_chars
        label.configure(text=f"Chars: {length}/{limit}")

    def _on_template_modified(self, index, box):
        if self._template_editing:
            box.edit_modified(False)
            return
        self._template_editing = True
        try:
            text = box.get("1.0", "end-1c")
            limit = self._template_max_chars
            if len(text) > limit:
                trimmed = text[:limit]
                box.delete("1.0", "end")
                box.insert("1.0", trimmed)
                text = trimmed
            self._update_template_counter(index, len(text))
        finally:
            box.edit_modified(False)
            self._template_editing = False

    def _on_template_tab_changed(self):
        self._schedule_save_message_templates()

    def _set_template_save_status(self, text, color="#8a8a8a"):
        if not hasattr(self, "template_save_status_label"):
            return
        if not self.template_save_status_label.winfo_exists():
            return
        self.template_save_status_label.configure(text=text, text_color=color)

    def _count_invites(self):
        return len(self._get_saved_invites())

    def refresh_workflow_status(self):
        accounts = self.db.get_accounts_overview()
        account_count = len(accounts)
        proxy_count = sum(1 for acc in accounts if acc[2])
        invite_count = self._count_invites()
        template_count = self._count_templates()
        settings_status = "tak" if self.settings_loaded else "nie"

        self.step_accounts_label.configure(text=f"1. Accounts: {account_count}")
        self.step_proxies_label.configure(text=f"2. Proxy: {proxy_count}")
        self.step_invites_label.configure(text=f"3. Servers: {invite_count}")
        self.step_templates_label.configure(text=f"4. Templates: {template_count}")
        self.step_settings_label.configure(text=f"5. Settings: {settings_status}")

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
            self.workflow_next_btn.grid()
            self.workflow_action_btn.grid_remove()
            return
        if stage == "join":
            self.workflow_next_btn.grid_remove()
            self.workflow_action_btn.configure(
                text="JOIN",
                fg_color="#f39c12",
                hover_color="#d35400",
                command=self._handle_join_action,
                state="normal" if joiner_enabled else "disabled",
            )
            self.workflow_action_btn.grid()
            return
        if stage == "scrape":
            self.workflow_next_btn.grid_remove()
            self.workflow_action_btn.configure(
                text="SCRAP MEMBERS",
                fg_color="#16a085",
                command=self._handle_scrape_action,
                state="normal" if scraper_enabled else "disabled",
            )
            self.workflow_action_btn.grid()
            return
        if stage == "dm":
            self.workflow_next_btn.grid_remove()
            self.workflow_action_btn.configure(
                text="Send DM",
                fg_color="#2ecc71",
                command=self._handle_dm_action,
                state="normal" if dm_enabled else "disabled",
            )
            self.workflow_action_btn.grid()

    def _handle_join_action(self):
        if not self._get_saved_invites():
            self.log_error("No saved invites. Open settings and save the server list.")
            return
        self.workflow_action_btn.configure(state="disabled")
        started = self.start_joining(on_complete=self._on_join_complete)
        if not started:
            self.workflow_action_btn.configure(state="normal")

    def _on_join_complete(self, success):
        def _update():
            if success:
                self._set_workflow_stage("scrape")
            else:
                self.log_error("Joining failed or was cancelled.")
                self.workflow_action_btn.configure(state="normal")
        self.after(0, _update)

    def _handle_scrape_action(self):
        self.workflow_action_btn.configure(state="disabled")
        started = self.start_guild_scraping(on_complete=self._on_scrape_complete)
        if not started:
            self.workflow_action_btn.configure(state="normal")

    def _on_scrape_complete(self, success):
        def _update():
            if success:
                self._set_workflow_stage("dm")
            else:
                self.log_error("Scraping failed or was cancelled.")
                self.workflow_action_btn.configure(state="normal")
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

    def _validate_update_endpoint(self, endpoint: str, input_widget=None):
        parsed = urlparse(endpoint)
        if parsed.scheme != "https":
            self._set_input_valid(input_widget, False)
            return False, "Version endpoint must use https."
        host = parsed.hostname
        if not host:
            self._set_input_valid(input_widget, False)
            return False, "Invalid version endpoint host."
        if host not in TRUSTED_UPDATE_HOSTS:
            self._set_input_valid(input_widget, False)
            return False, f"Nieufny host endpointu: {host}."
        self._set_input_valid(input_widget, True)
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
        self.notification_var.set("Notifications: --")
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
        recent_rate = data.get("recent_rate_limit_count", 0)
        recent_server = data.get("recent_server_error_count", 0)
        window_seconds = int(data.get("alert_window_seconds", 60))

        self.health_uptime_label.configure(text=f"Uptime: {uptime}")
        self.health_avg_label.configure(text=f"Avg request: {avg_ms:.1f} ms")
        if last_rate is not None:
            last_text = self._format_duration(last_rate)
            self.health_rate_label.configure(text=f"Rate limits: {rate_count} (last {last_text} ago)")
        else:
            self.health_rate_label.configure(text=f"Rate limits: {rate_count}")
        self.health_requests_label.configure(text=f"Requests: {data['total_requests']}")
        alerts = []
        if recent_rate >= 5:
            alerts.append(f"High 429 rate ({recent_rate}/{window_seconds}s)")
        if recent_server >= 3:
            alerts.append(f"5xx errors ({recent_server}/{window_seconds}s)")
        if alerts:
            color = "#e74c3c" if recent_server >= 3 else "#f39c12"
            self.health_alert_label.configure(text=f"Alerts: {'; '.join(alerts)}", text_color=color)
        else:
            self.health_alert_label.configure(text="Alerts: none", text_color="#8a8a8a")

        self.after(1000, self.refresh_health_metrics)

    def save_version_settings(self):
        endpoint_entry = None
        if hasattr(self, "version_endpoint_input") and self.version_endpoint_input.winfo_exists():
            endpoint_entry = self.version_endpoint_input
        endpoint = endpoint_entry.get().strip() if endpoint_entry else ""
        if endpoint:
            ok, err = self._validate_update_endpoint(endpoint, endpoint_entry)
            if not ok:
                self.log_error(err)
                self.show_notification(err, level="error")
                return
            self.db.set_setting("version_endpoint", endpoint)
            self.add_log("[Settings] Zapisano endpoint wersji.")
        else:
            self._set_input_valid(endpoint_entry, True)
            self.db.set_setting("version_endpoint", None)
            self.add_log("[Settings] Removed version endpoint.")

    def check_for_updates(self):
        endpoint_entry = None
        if hasattr(self, "version_endpoint_input") and self.version_endpoint_input.winfo_exists():
            endpoint_entry = self.version_endpoint_input
        endpoint = endpoint_entry.get().strip() if endpoint_entry else ""
        if not endpoint:
            self._set_input_valid(endpoint_entry, False)
            self.log_error("Version endpoint is empty. Fill it in settings.")
            self.show_notification("Missing version endpoint. Update settings.", level="error")
            return
        ok, err = self._validate_update_endpoint(endpoint, endpoint_entry)
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
            self.add_log(f"[Updater] Failed to fetch version: {exc}")
            self.after(0, lambda: self._set_version_status("Last check: error"))
            self.after(0, lambda: self._handle_update_error("Failed to fetch version info."))
            return

        updater = UpdateManager(Path(__file__).resolve().parent, self.add_log)
        try:
            updater._require_valid_signature(data)
        except UpdateError as exc:
            self.add_log(f"[Updater] Invalid signature: {exc}")
            self.after(0, lambda: self._set_version_status("Last check: invalid signature"))
            self.after(0, lambda: self._handle_update_error("Invalid update signature."))
            return

        latest = data.get("latest_version") or data.get("version")
        download_url = data.get("download_url") or data.get("url")
        files_payload = data.get("files")
        if not latest:
            self.add_log("[Updater] Missing latest_version/version field in response.")
            self.after(0, lambda: self._set_version_status("Last check: invalid response"))
            return

        comparison = self._compare_versions(APP_VERSION, latest)
        if comparison is None:
            message = f"[Updater] Current version: {APP_VERSION}, latest: {latest}."
        elif comparison < 0:
            message = f"[Updater] Update available: {latest} (current {APP_VERSION})."
        elif comparison == 0:
            message = f"[Updater] You are on the latest version ({APP_VERSION})."
        else:
            message = f"[Updater] Local version ({APP_VERSION}) is newer than {latest}."
        if download_url:
            message += f" Link: {download_url}"
        self.add_log(message)
        update_available = comparison is not None and comparison < 0 and (download_url or files_payload)
        if update_available:
            self.update_info = data
            self.after(0, lambda: self._set_update_button_state(True))
            self.after(0, lambda: self._set_update_status("Update: available"))
        else:
            self.update_info = None
            self.after(0, lambda: self._set_update_button_state(False))
            self.after(0, lambda: self._set_update_status("Update: none"))
        self.after(0, lambda: self._set_version_status(f"Last check: {latest}"))
        self.after(0, lambda: self._handle_update_result(latest, update_available))

    def _handle_update_result(self, latest, update_available):
        if update_available:
            self.show_notification(
                f"Update available ({latest}). You can download it manually.",
                level="warning",
            )
        else:
            self.show_notification("No new updates.", level="success")

        if self.manual_update_requested:
            self.manual_update_requested = False
            if update_available:
                confirmed = messagebox.askyesno(
                    "Update",
                    f"Update {latest} is available. Download and install now?",
                )
                if confirmed:
                    self.download_update()
            else:
                messagebox.showinfo("Update", "No updates available.")

    def _handle_update_error(self, message):
        self.show_notification(message, level="error")
        if self.manual_update_requested:
            self.manual_update_requested = False
            messagebox.showerror("Update", message)

    def download_update(self):
        endpoint_entry = None
        if hasattr(self, "version_endpoint_input") and self.version_endpoint_input.winfo_exists():
            endpoint_entry = self.version_endpoint_input
        endpoint = endpoint_entry.get().strip() if endpoint_entry else ""
        if not endpoint:
            self._set_input_valid(endpoint_entry, False)
            self.log_error("Version endpoint is empty. Fill it in settings.")
            self.show_notification("Missing version endpoint. Update settings.", level="error")
            return
        ok, err = self._validate_update_endpoint(endpoint, endpoint_entry)
        if not ok:
            self.log_error(err)
            self.show_notification(err, level="error")
            return
        self.db.set_setting("version_endpoint", endpoint)
        self.add_log("[Updater] Downloading update...")
        self._set_update_button_state(False)
        self._set_update_status("Update: downloading...")
        self.show_notification("Downloading update...", level="info")
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
            self.add_log(f"[Updater] Failed to download update: {exc}")
            self.after(0, lambda: self._set_update_status("Update: download error"))
            self.after(0, lambda: self._set_update_button_state(True))
            self.after(0, lambda: self.show_notification("Update download error.", level="error"))
            return

        updater = UpdateManager(Path(__file__).resolve().parent, self.add_log)
        try:
            updater.download_and_apply(data)
        except UpdateError as exc:
            self.add_log(f"[Updater] Update failed: {exc}")
            self.after(0, lambda: self._set_update_status("Update: validation error"))
            self.after(0, lambda: self._set_update_button_state(True))
            self.after(0, lambda: self.show_notification("Update failed.", level="error"))
            return

        self.add_log("[Updater] Update finished. Restart the app.")
        self.after(0, lambda: self._set_update_status("Update: installed"))
        self.after(0, lambda: self.show_notification("Update installed. Restart the app.", level="success"))

    def validate_token_format(self, token, input_widget=None):
        if not token:
            self.log_error("Invalid token: empty field.")
            self._set_input_valid(input_widget, False)
            return False
        token_pattern = re.compile(r"^[A-Za-z0-9_\-]+\.[A-Za-z0-9_\-]+\.[A-Za-z0-9_\-]+$")
        if not token_pattern.match(token) or len(token) < 50:
            self.log_error("Invalid token format.")
            self._set_input_valid(input_widget, False)
            return False
        self._set_input_valid(input_widget, True)
        return True

    def validate_proxy(self, proxy, input_widget=None):
        if not proxy:
            self._set_input_valid(input_widget, True)
            return True
        parsed = urlparse(proxy)
        if parsed.scheme and parsed.hostname and parsed.port:
            if parsed.scheme not in {"http", "https", "socks5"}:
                self.log_error("Invalid proxy format.")
                self._set_input_valid(input_widget, False)
                return False
            self._set_input_valid(input_widget, True)
            return True
        parsed = urlparse(f"http://{proxy}")
        if parsed.hostname and parsed.port:
            self._set_input_valid(input_widget, True)
            return True
        self.log_error("Invalid proxy format.")
        self._set_input_valid(input_widget, False)
        return False

    def normalize_invite(self, invite):
        invite = invite.strip()
        if not invite:
            return None
        if invite.startswith("<") and invite.endswith(">"):
            invite = invite[1:-1].strip()
        parsed = urlparse(invite if "://" in invite else f"https://{invite}")
        host = (parsed.hostname or "").lower()
        path = parsed.path.strip("/")
        code = None
        if host in {"discord.gg", "www.discord.gg"}:
            if path:
                code = path.split("/")[0]
        elif host in {"discord.com", "www.discord.com", "discordapp.com", "www.discordapp.com"}:
            if path.startswith("invite/"):
                code = path.split("/", 1)[1]
                if code:
                    code = code.split("/")[0]
        if code and re.match(r"^[A-Za-z0-9-]{2,}$", code):
            return code
        if re.match(r"^[A-Za-z0-9-]{2,}$", invite):
            return invite
        return None

    def is_valid_channel_id(self, channel_id, input_widget=None):
        if not channel_id:
            self.log_error("Invalid Channel ID: empty field.")
            self._set_input_valid(input_widget, False)
            return False
        if not re.match(r"^\d{17,20}$", channel_id):
            self.log_error("Invalid Channel ID format.")
            self._set_input_valid(input_widget, False)
            return False
        self._set_input_valid(input_widget, True)
        return True

    def _get_scrape_limit(self, raw_value, default_limit, input_widget=None):
        value = (raw_value or "").strip()
        if not value:
            self._set_input_valid(input_widget, True)
            return default_limit
        try:
            limit = int(value)
        except ValueError:
            self.log_error("Invalid Range format. Use an integer.")
            self._set_input_valid(input_widget, False)
            return None
        if limit <= 0:
            self.log_error("Range must be greater than zero.")
            self._set_input_valid(input_widget, False)
            return None
        self._set_input_valid(input_widget, True)
        return limit

    def is_valid_guild_id(self, guild_id, input_widget=None):
        if not guild_id:
            self.log_error("Invalid Guild ID: empty field.")
            self._set_input_valid(input_widget, False)
            return False
        if not re.match(r"^\d{17,20}$", guild_id):
            self.log_error("Invalid Guild ID format.")
            self._set_input_valid(input_widget, False)
            return False
        self._set_input_valid(input_widget, True)
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
        normalized = message.strip()
        lowered = normalized.casefold()
        if lowered.startswith("[error]") or lowered.startswith("error:") or lowered.startswith("[!]"):
            return "Error"
        if lowered.startswith("[warn]") or lowered.startswith("[warning]") or lowered.startswith("warning:"):
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
        if self._is_proxy_required() and not proxy:
            self.log_error("Proxy is required.")
            self._set_input_valid(self.proxy_input, False)
            return
        if not self.validate_token_format(token, self.token_input):
            return
        if not self.validate_proxy(proxy, self.proxy_input):
            return
        try:
            dm_limit = int(dm_limit_raw) if dm_limit_raw else 15
        except ValueError:
            self.log_error("DM limit must be an integer.")
            self._set_input_valid(self.dm_limit_input, False)
            return
        try:
            join_limit = int(join_limit_raw) if join_limit_raw else 5
        except ValueError:
            self.log_error("Join limit must be an integer.")
            self._set_input_valid(self.join_limit_input, False)
            return
        if dm_limit <= 0 or join_limit <= 0:
            self.log_error("Limits must be greater than zero.")
            if dm_limit <= 0:
                self._set_input_valid(self.dm_limit_input, False)
            if join_limit <= 0:
                self._set_input_valid(self.join_limit_input, False)
            return
        self._set_input_valid(self.dm_limit_input, True)
        self._set_input_valid(self.join_limit_input, True)
        self.add_acc_btn.configure(state="disabled")
        thread = threading.Thread(
            target=self._add_account_worker,
            args=(token, proxy, dm_limit, join_limit),
            daemon=True,
        )
        thread.start()

    def _add_account_worker(self, token, proxy, dm_limit, join_limit):
        if self._is_proxy_required():
            ok, err = self._check_proxy_alive(proxy)
            if not ok:
                status, info = "retry", err
            else:
                status, info = self.token_manager.validate_token(token, proxy)
        else:
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
                self.log_error(f"Invalid token: {info}.")
                return

            initial_status = "Active" if status == "ok" else "Unverified"
            account_id = self.db.add_account("discord", token, proxy, dm_limit, join_limit, initial_status)
            if not account_id:
                self.log_error("Account already exists or token is invalid.")
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
            self.add_log(f"[Accounts] Account {account_id}: still unverified, stopping retries.")
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
            self.add_log(f"[Accounts] Account {account_id} verified: {info}.")
            self.refresh_accounts_overview()
            return
        if status == "unauthorized":
            self.log_error(f"[Accounts] Account {account_id} invalid: {info}. Removing.")
            self.db.update_account_status(account_id, "Banned/Dead")
            self.db.remove_account(account_id)
            self.refresh_accounts_overview()
            return
        self.add_log(f"[Accounts] Account {account_id}: verification deferred ({info}).")
        self._schedule_account_recheck(account_id, token, proxy, attempt=attempt + 1)

    def _reset_add_account_form(self):
        self.token_input.delete(0, "end")
        self.proxy_input.delete(0, "end")
        self.dm_limit_input.delete(0, "end")
        self.dm_limit_input.insert(0, "15")
        self.join_limit_input.delete(0, "end")
        self.join_limit_input.insert(0, "5")
        self._set_input_valid(self.token_input, True)
        self._set_input_valid(self.proxy_input, True)
        self._set_input_valid(self.dm_limit_input, True)
        self._set_input_valid(self.join_limit_input, True)

    def _get_invite_list(self, raw_text=None, log_invalid=True):
        if raw_text is None:
            if not hasattr(self, "invite_input"):
                return []
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
        if invalid and log_invalid:
            self.log_error(f"Invalid invites (skipping): {', '.join(invalid)}")
        return normalized

    def _get_saved_invites(self):
        raw_text = self.db.get_setting("join_invites", "")
        return self._get_invite_list(raw_text, log_invalid=False)

    def save_invite_settings(self):
        if not hasattr(self, "invite_input"):
            return
        raw_text = self.invite_input.get("1.0", "end").strip()
        if not raw_text:
            self.db.set_setting("join_invites", None)
            self.add_log("[Joiner] Cleared invite list.")
            self.refresh_workflow_status()
            return
        invites = self._get_invite_list(raw_text, log_invalid=True)
        if not invites:
            self.log_error("No valid invites to save.")
            return
        self.db.set_setting("join_invites", "\n".join(invites))
        self.add_log(f"[Joiner] Saved {len(invites)} invites.")
        self.save_joiner_settings()
        self.refresh_workflow_status()

    def _load_invite_settings(self):
        if not hasattr(self, "invite_input"):
            return
        stored = self.db.get_setting("join_invites", "")
        self.invite_input.delete("1.0", "end")
        if stored:
            self.invite_input.insert("1.0", stored)

    def _parse_role_ids(self, raw_text):
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

    def save_joiner_settings(self):
        if not hasattr(self, "onboarding_role_whitelist_input"):
            return
        raw = self.onboarding_role_whitelist_input.get("1.0", "end").strip()
        if not raw:
            self.db.set_setting("onboarding_role_whitelist", None)
            return
        role_ids, invalid = self._parse_role_ids(raw)
        if invalid:
            self.log_warning(f"Invalid role IDs (skipping): {', '.join(invalid)}")
        if role_ids:
            self.db.set_setting("onboarding_role_whitelist", "\n".join(role_ids))
            self.add_log(f"[Joiner] Saved {len(role_ids)} role IDs for onboarding.")
        else:
            self.db.set_setting("onboarding_role_whitelist", None)

    def _load_joiner_settings(self):
        if not hasattr(self, "onboarding_role_whitelist_input"):
            return
        stored = self.db.get_setting("onboarding_role_whitelist", "")
        self.onboarding_role_whitelist_input.delete("1.0", "end")
        if stored:
            self.onboarding_role_whitelist_input.insert("1.0", stored)

    def test_proxy_settings(self):
        proxy = ""
        if hasattr(self, "proxy_input") and self.proxy_input.winfo_exists():
            proxy = self.proxy_input.get().strip()
        if not proxy:
            self.log_error("Proxy is empty.")
            if hasattr(self, "proxy_input"):
                self._set_input_valid(self.proxy_input, False)
            return
        if not self.validate_proxy(proxy, self.proxy_input if hasattr(self, "proxy_input") else None):
            return
        ok, err = self._check_proxy_alive(proxy)
        if ok:
            self.add_log("[Proxy] Proxy check passed.")
            if hasattr(self, "proxy_input"):
                self._set_input_valid(self.proxy_input, True)
        else:
            self.log_error(f"[Proxy] {err}")
            if hasattr(self, "proxy_input"):
                self._set_input_valid(self.proxy_input, False)

    def test_all_account_proxies(self):
        accounts = self.db.get_active_accounts("discord")
        if not accounts:
            self.log_error("[Proxy] No active accounts to test.")
            return
        total = len(accounts)
        ok_count = 0
        bad = []
        for acc_id, _, _, proxy, _, _, _, _, _, _, _ in accounts:
            if not proxy:
                bad.append(f"{acc_id}: missing proxy")
                continue
            if not self.validate_proxy(proxy):
                bad.append(f"{acc_id}: invalid proxy")
                continue
            ok, err = self._check_proxy_alive(proxy)
            if ok:
                ok_count += 1
            else:
                bad.append(f"{acc_id}: {err}")
        if ok_count == total:
            self.add_log(f"[Proxy] All proxies OK ({ok_count}/{total}).")
            return
        self.log_warning(f"[Proxy] Proxies OK: {ok_count}/{total}. Failed: {len(bad)}.")
        if bad:
            preview = "; ".join(bad[:10])
            suffix = "..." if len(bad) > 10 else ""
            self.log_warning(f"[Proxy] Failures: {preview}{suffix}")

    def start_joining(self, on_complete=None):
        if not self.module_vars["joiner"].get():
            self.log_error("Joiner module is disabled.")
            return False
        invites = self._get_saved_invites()
        if not invites:
            self.log_error("No saved invites. Open settings and save the server list.")
            return False
        if hasattr(self, "auto_accept_rules_var"):
            auto_accept_rules = self.auto_accept_rules_var.get()
        else:
            auto_accept_rules = self._get_setting_bool("auto_accept_rules", True)
        if hasattr(self, "auto_onboarding_var"):
            auto_onboarding = self.auto_onboarding_var.get()
        else:
            auto_onboarding = self._get_setting_bool("auto_onboarding", True)
        if hasattr(self, "onboarding_role_whitelist_input") and self.onboarding_role_whitelist_input.winfo_exists():
            raw_roles = self.onboarding_role_whitelist_input.get("1.0", "end")
        else:
            raw_roles = self.db.get_setting("onboarding_role_whitelist", "")
        role_whitelist = []
        if raw_roles:
            role_whitelist, invalid = self._parse_role_ids(raw_roles)
            if invalid:
                self.log_warning(f"Invalid role IDs (skipping): {', '.join(invalid)}")
        if hasattr(self, "join_delay_min_input") and self.join_delay_min_input.winfo_exists():
            join_delay = self._parse_delay_range(
                self.join_delay_min_input,
                self.join_delay_max_input,
                "Join delay (h)",
                cast_type=float,
                min_value=0.0,
            )
        else:
            join_delay = self._parse_delay_range_from_settings(
                "join_delay_min",
                "join_delay_max",
                "Join delay (h)",
                cast_type=float,
                min_value=0.0,
                assume_seconds=True,
            )
        if not join_delay:
            return False
        join_delay_min_h, join_delay_max_h = join_delay
        self.db.set_setting("join_delay_min", str(join_delay_min_h))
        self.db.set_setting("join_delay_max", str(join_delay_max_h))
        self._set_setting_bool("delay_units_hours", True)
        join_delay_min = int(round(self._hours_to_seconds(join_delay_min_h)))
        join_delay_max = int(round(self._hours_to_seconds(join_delay_max_h)))
        accounts = self.db.get_active_accounts("discord")
        def _join_with_proxy_check():
            valid_accounts = self._ensure_account_proxies(accounts, "join")
            if not valid_accounts:
                if on_complete:
                    on_complete(False)
                return
            allowed_ids = {acc[0] for acc in valid_accounts}
            self.joiner.run_mass_join(
                invites,
                join_delay_min,
                join_delay_max,
                on_complete,
                auto_accept_rules,
                auto_onboarding,
                role_whitelist,
                allowed_ids,
            )
        thread = threading.Thread(target=_join_with_proxy_check)
        thread.daemon = True
        thread.start()
        return True

    def start_scraping(self, on_complete=None):
        if not self.module_vars["scraper"].get():
            self.log_error("Scraper module is disabled.")
            return False
        token_entry = self.token_input if hasattr(self, "token_input") and self.token_input.winfo_exists() else None
        if token_entry:
            token = token_entry.get().strip()
        else:
            token = self.db.get_setting("scrape_token", "").strip()
        if not token:
            self.log_error("Open settings and enter a scraping token.")
            return False
        channel_entry = (
            self.scrape_channel_input
            if hasattr(self, "scrape_channel_input") and self.scrape_channel_input.winfo_exists()
            else None
        )
        if channel_entry:
            channel_id = channel_entry.get().strip()
        else:
            channel_id = self.db.get_setting("scrape_channel_id", "").strip()
        range_entry = (
            self.scrape_range_input
            if hasattr(self, "scrape_range_input") and self.scrape_range_input.winfo_exists()
            else None
        )
        if range_entry:
            range_value = range_entry.get().strip()
        else:
            range_value = self.db.get_setting("scrape_range", "").strip()
        if not self.validate_token_format(token, token_entry):
            return False
        if not self.is_valid_channel_id(channel_id, channel_entry):
            return False
        limit = self._get_scrape_limit(range_value, 500, range_entry)
        if limit is None:
            return False
        if channel_entry:
            self.save_scrape_settings()
        scrape_proxy = ""
        if hasattr(self, "scrape_proxy_input") and self.scrape_proxy_input.winfo_exists():
            scrape_proxy = self.scrape_proxy_input.get().strip()
        else:
            scrape_proxy = self.db.get_setting("scrape_proxy", "").strip()
        def _scrape_with_proxy_check():
            if not self._ensure_scraper_proxy(scrape_proxy, "channel scrape"):
                if on_complete:
                    on_complete(False)
                return
            self.scraper.scrape_history(token, channel_id, limit, on_complete, proxy=scrape_proxy)
        thread = threading.Thread(
            target=_scrape_with_proxy_check,
        )
        thread.daemon = True
        thread.start()
        return True

    def start_guild_scraping(self, on_complete=None):
        if not self.module_vars["scraper"].get():
            self.log_error("Scraper module is disabled.")
            return False
        token_entry = self.token_input if hasattr(self, "token_input") and self.token_input.winfo_exists() else None
        if token_entry:
            token = token_entry.get().strip()
        else:
            token = self.db.get_setting("scrape_token", "").strip()
        if not token:
            self.log_error("Open settings and enter a scraping token.")
            return False
        guild_entry = (
            self.scrape_guild_input
            if hasattr(self, "scrape_guild_input") and self.scrape_guild_input.winfo_exists()
            else None
        )
        if guild_entry:
            guild_id = guild_entry.get().strip()
        else:
            guild_id = self.db.get_setting("scrape_guild_id", "").strip()
        range_entry = (
            self.scrape_range_input
            if hasattr(self, "scrape_range_input") and self.scrape_range_input.winfo_exists()
            else None
        )
        if range_entry:
            range_value = range_entry.get().strip()
        else:
            range_value = self.db.get_setting("scrape_range", "").strip()
        if not self.validate_token_format(token, token_entry):
            return False
        if not self.is_valid_guild_id(guild_id, guild_entry):
            return False
        limit = self._get_scrape_limit(range_value, 1000, range_entry)
        if limit is None:
            return False
        if guild_entry:
            self.save_scrape_settings()
        scrape_proxy = ""
        if hasattr(self, "scrape_proxy_input") and self.scrape_proxy_input.winfo_exists():
            scrape_proxy = self.scrape_proxy_input.get().strip()
        else:
            scrape_proxy = self.db.get_setting("scrape_proxy", "").strip()
        def _scrape_with_proxy_check():
            if not self._ensure_scraper_proxy(scrape_proxy, "guild scrape"):
                if on_complete:
                    on_complete(False)
                return
            self.scraper.scrape_guild_members(token, guild_id, limit, on_complete, proxy=scrape_proxy)
        thread = threading.Thread(
            target=_scrape_with_proxy_check,
        )
        thread.daemon = True
        thread.start()
        return True

    def start_status_update(self):
        if not self.module_vars["status"].get():
            self.log_error("Status module is disabled.")
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
        accounts = self.db.get_active_accounts("discord")
        def _status_with_proxy_check():
            valid_accounts = self._ensure_account_proxies(accounts, "status update")
            if not valid_accounts:
                return
            allowed_ids = {acc[0] for acc in valid_accounts}
            self.status_changer.run_auto_update(
                status_type,
                custom_text,
                status_delay_min,
                status_delay_max,
                allowed_ids,
            )
        thread = threading.Thread(
            target=_status_with_proxy_check,
        )
        thread.daemon = True
        thread.start()
        self._update_auto_status_label()

    def stop_status_update(self):
        self.status_changer.stop()
        self.add_log("[Status] Automatyczna zmiana statusu zatrzymana.")
        self._update_auto_status_label()

    def _update_auto_status_label(self):
        if not hasattr(self, "status_state_label") or not self.status_state_label.winfo_exists():
            return
        running = bool(self.status_changer.auto_running)
        if running:
            text = "Auto status: running"
            color = "#2ecc71"
        elif not self.module_vars["status"].get():
            text = "Auto status: disabled"
            color = "#e67e22"
        else:
            text = "Auto status: stopped"
            color = "#8a8a8a"
        self.status_state_label.configure(text=text, text_color=color)

    def start_mission(self):
        if not self.module_vars["dm"].get():
            self.log_error("DM module is disabled.")
            return
        templates = [tpl for tpl in self._get_message_templates() if tpl.strip()]
        if not templates:
            self.log_error("No valid message templates.")
            return
        if hasattr(self, "dm_delay_min_input") and self.dm_delay_min_input.winfo_exists():
            dm_delay = self._parse_delay_range(
                self.dm_delay_min_input,
                self.dm_delay_max_input,
                "DM delay (h)",
                cast_type=float,
                min_value=0.0,
            )
        else:
            dm_delay = self._parse_delay_range_from_settings(
                "dm_delay_min",
                "dm_delay_max",
                "DM delay (h)",
                cast_type=float,
                min_value=0.0,
                assume_seconds=True,
            )
        if not dm_delay:
            return
        if hasattr(self, "friend_delay_min_input") and self.friend_delay_min_input.winfo_exists():
            friend_delay = self._parse_delay_range(
                self.friend_delay_min_input,
                self.friend_delay_max_input,
                "Friend request delay (h)",
                cast_type=float,
                min_value=0.0,
            )
        else:
            friend_delay = self._parse_delay_range_from_settings(
                "friend_delay_min",
                "friend_delay_max",
                "Friend request delay (h)",
                cast_type=float,
                min_value=0.0,
                assume_seconds=True,
            )
        if not friend_delay:
            return
        if hasattr(self, "account_min_interval_input") and self.account_min_interval_input.winfo_exists():
            account_min_interval = self._parse_min_value(
                self.account_min_interval_input,
                "Account min interval (h)",
                cast_type=float,
                min_value=0.0,
            )
        else:
            account_min_interval = self._parse_min_value_from_settings(
                "account_min_interval",
                "Account min interval (h)",
                cast_type=float,
                min_value=0.0,
                assume_seconds=True,
            )
        if account_min_interval is None:
            return
        if hasattr(self, "target_min_interval_input") and self.target_min_interval_input.winfo_exists():
            target_min_interval = self._parse_min_value(
                self.target_min_interval_input,
                "Target min interval (h)",
                cast_type=float,
                min_value=0.0,
            )
        else:
            target_min_interval = self._parse_min_value_from_settings(
                "target_min_interval",
                "Target min interval (h)",
                cast_type=float,
                min_value=0.0,
                assume_seconds=True,
            )
        if target_min_interval is None:
            return
        dm_delay_min_h, dm_delay_max_h = dm_delay
        friend_delay_min_h, friend_delay_max_h = friend_delay
        self.db.set_setting("dm_delay_min", str(dm_delay_min_h))
        self.db.set_setting("dm_delay_max", str(dm_delay_max_h))
        self.db.set_setting("friend_delay_min", str(friend_delay_min_h))
        self.db.set_setting("friend_delay_max", str(friend_delay_max_h))
        self.db.set_setting("account_min_interval", str(account_min_interval))
        self.db.set_setting("target_min_interval", str(target_min_interval))
        self._set_setting_bool("delay_units_hours", True)
        dm_delay_min = int(round(self._hours_to_seconds(dm_delay_min_h)))
        dm_delay_max = int(round(self._hours_to_seconds(dm_delay_max_h)))
        friend_delay_min = int(round(self._hours_to_seconds(friend_delay_min_h)))
        friend_delay_max = int(round(self._hours_to_seconds(friend_delay_max_h)))
        account_min_interval = int(round(self._hours_to_seconds(account_min_interval)))
        target_min_interval = int(round(self._hours_to_seconds(target_min_interval)))
        use_friend_req = self.friend_request_var.get()
        dry_run = self.dry_run_var.get()
        if dry_run:
            self.add_log("[Mission] Dry-run enabled. Messages will not be sent.")
        accounts = self.db.get_active_accounts("discord")
        def _mission_with_proxy_check():
            valid_accounts = self._ensure_account_proxies(accounts, "mission")
            if not valid_accounts:
                return
            allowed_ids = {acc[0] for acc in valid_accounts}
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
        thread = threading.Thread(
            target=_mission_with_proxy_check,
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
            self.log_error("Enter an account ID to remove.")
            self._set_input_valid(self.remove_account_input, False)
            return
        try:
            account_id = int(raw_id)
        except ValueError:
            self.log_error("Account ID must be a number.")
            self._set_input_valid(self.remove_account_input, False)
            return
        self._set_input_valid(self.remove_account_input, True)
        self.db.remove_account(account_id)
        self.add_log(f"[Accounts] Removed account {account_id}.")
        self.remove_account_input.delete(0, "end")
        self.refresh_accounts_overview()

    def refresh_accounts_overview(self):
        accounts = self.db.get_accounts_overview()
        self.acc_overview_box.delete("1.0", "end")
        if not accounts:
            self.acc_overview_box.insert("end", "No accounts in database.\n")
            self.refresh_workflow_status()
            return
        self.acc_overview_box.tag_config("status_active", foreground=self.account_status_colors["active"])
        self.acc_overview_box.tag_config("status_unverified", foreground=self.account_status_colors["unverified"])
        self.acc_overview_box.tag_config("status_banned", foreground=self.account_status_colors["banned"])
        self.acc_overview_box.insert("end", "ID | Status | DM sent/limit | Join sent/limit | Proxy\n")
        self.acc_overview_box.insert("end", "-" * 70 + "\n")
        for acc_id, status, proxy, dm_limit, sent_today, join_limit, join_today in accounts:
            proxy_value = proxy if proxy else "-"
            line = f"{acc_id} | {status} | {sent_today}/{dm_limit} | {join_today}/{join_limit} | {proxy_value}\n"
            status_key = (status or "").strip().casefold()
            if status_key == "active":
                tag = "status_active"
            elif status_key == "unverified":
                tag = "status_unverified"
            elif status_key in {"banned", "dead", "banned/dead"}:
                tag = "status_banned"
            else:
                tag = None
            if tag:
                start_index = self.acc_overview_box.index("end-1c")
                self.acc_overview_box.insert("end", line)
                end_index = self.acc_overview_box.index("end-1c")
                self.acc_overview_box.tag_add(tag, start_index, end_index)
            else:
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
            self.log_error(f"Unable to read file: {exc}")
            return [], []
        unique_ids = list(dict.fromkeys(ids))
        return unique_ids, invalid

    def add_targets_from_input(self):
        raw = self.target_input.get("1.0", "end")
        ids, invalid = self._parse_user_ids(raw)
        if not ids:
            self.log_error("No valid IDs to add.")
            return
        if invalid:
            self.log_error(f"Invalid IDs (skipping): {', '.join(invalid)}")
        self.db.add_targets(ids, "discord")
        self.add_log(f"[Targets] Added {len(ids)} targets.")
        self.target_input.delete("1.0", "end")
        self.refresh_targets_overview()

    def import_targets_from_file(self):
        file_path = filedialog.askopenfilename(
            title="Select a .txt file with IDs",
            filetypes=[("Text files", "*.txt"), ("All files", "*.*")],
        )
        if not file_path:
            return
        ids, invalid = self._parse_user_ids_from_file(file_path)
        if not ids:
            self.log_error("No valid IDs in file.")
            return
        if invalid:
            self.log_error(f"Invalid IDs (skipping): {', '.join(invalid)}")
        self.db.add_targets(ids, "discord")
        self.add_log(f"[Targets] Imported {len(ids)} targets from file.")
        self.refresh_targets_overview()

    def clear_targets(self):
        self.db.clear_targets()
        self.add_log("[Targets] Target list cleared.")
        self.refresh_targets_overview()

    def refresh_targets_overview(self):
        counts, total = self.db.get_target_counts()
        pending = counts.get("Pending", 0)
        sent = counts.get("Sent", 0)
        failed = counts.get("Failed", 0)
        dry_run = counts.get("Dry-Run", 0)
        retrying = counts.get("Retry", 0)
        self.target_summary_label.configure(
            text=(
                f"Targets: {total} | Pending: {pending} | Retry: {retrying} | "
                f"Sent: {sent} | Failed: {failed} | Dry-Run: {dry_run}"
            )
        )
        targets = self.db.get_targets(limit=50)
        self.target_overview_box.delete("1.0", "end")
        if not targets:
            self.target_overview_box.insert("end", "No targets in database.\n")
            return
        for user_id, status in targets:
            self.target_overview_box.insert("end", f"{user_id} | {status}\n")

    def refresh_banlist_overview(self):
        self.banlist_box.delete("1.0", "end")
        if not self.banlist_path.exists():
            self.banlist_summary_label.configure(text="Banlist: 0")
            self.banlist_box.insert("end", "No entries in banlist.\n")
            return
        try:
            lines = self.banlist_path.read_text(encoding="utf-8").splitlines()
        except OSError as exc:
            self.log_error(f"Unable to read banlist: {exc}")
            self.banlist_summary_label.configure(text="Banlist: ?")
            self.banlist_box.insert("end", "Failed to read banlist.\n")
            return
        entries = [line for line in lines if line.strip()]
        self.banlist_summary_label.configure(text=f"Banlist: {len(entries)}")
        if not entries:
            self.banlist_box.insert("end", "No entries in banlist.\n")
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
        status = "enabled" if self.export_banned_tokens_plaintext_var.get() else "disabled"
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
        if hasattr(self, "dry_run_toggle"):
            self.dry_run_toggle.configure(state="normal" if dm_enabled else "disabled")
        if hasattr(self, "invite_save_btn"):
            self.invite_save_btn.configure(state="normal")
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
        self._update_auto_status_label()

    def open_settings_window(self):
        if self.settings_window and self.settings_window.winfo_exists():
            try:
                self.settings_window.lift()
                self.settings_window.focus_force()
            except Exception:
                pass
            self.settings_window.focus()
            return
        self._unbind_settings_scroll_fix()
        self.settings_window = ctk.CTkToplevel(self)
        self.settings_window.title("Configuration")
        self.settings_window.geometry("900x900")
        try:
            self.settings_window.transient(self)
            self.settings_window.lift()
            self.settings_window.focus_force()
            self.settings_window.attributes("-topmost", True)
            self.after(200, lambda: self.settings_window.attributes("-topmost", False))
        except Exception:
            pass
        self.settings_window.grid_columnconfigure(0, weight=1)
        self.settings_window.grid_rowconfigure(0, weight=1)
        self.settings_container = ctk.CTkScrollableFrame(self.settings_window, fg_color="transparent")
        self.settings_container.grid(row=0, column=0, padx=20, pady=20, sticky="nsew")
        self._build_settings_sections(self.settings_container)
        self._install_settings_scroll_fix()
        self._schedule_settings_canvas_refresh()
        self.settings_loaded = True
        self.refresh_workflow_status()
        self.settings_window.protocol("WM_DELETE_WINDOW", self.close_settings_window)

    def close_settings_window(self):
        if self.settings_window and self.settings_window.winfo_exists():
            self.save_delay_settings()
            self.save_message_templates()
            self.save_scrape_settings()
            self.save_invite_settings()
            self.save_captcha_settings()
            self.save_version_settings()
            self.save_status_settings()
            self.save_profile_settings()
            self.save_joiner_settings()
            self._set_setting_bool("use_friend_request", self.friend_request_var.get())
            self._set_setting_bool("dry_run", self.dry_run_var.get())
            self._set_setting_bool("auto_accept_rules", self.auto_accept_rules_var.get())
            self._set_setting_bool("auto_onboarding", self.auto_onboarding_var.get())
            self._set_setting_bool("require_proxy", self.require_proxy_var.get())
            self.settings_window.destroy()
            self.refresh_workflow_status()
        self.settings_window = None
        self._unbind_settings_scroll_fix()

    def _install_settings_scroll_fix(self):
        if self._settings_scroll_fix_bound:
            return
        if not self.settings_window:
            return
        self._settings_scroll_fix_bind_id = self.settings_window.bind(
            "<MouseWheel>", self._settings_scroll_fix, add="+"
        )
        self._settings_scroll_fix_bound = True
        container = getattr(self, "settings_container", None)
        if not container:
            return
        scrollbar = getattr(container, "_scrollbar", None)
        if not scrollbar:
            return
        self._settings_scrollbar_original = scrollbar.cget("command")
        self._settings_scrollbar_widget = scrollbar
        scrollbar.configure(command=self._settings_scrollbar_command)

    def _unbind_settings_scroll_fix(self):
        if not self._settings_scroll_fix_bound:
            return
        try:
            if self.settings_window and self._settings_scroll_fix_bind_id:
                self.settings_window.unbind("<MouseWheel>", self._settings_scroll_fix_bind_id)
        except Exception:
            pass
        self._settings_scroll_fix_bound = False
        self._settings_scroll_fix_bind_id = None
        if self._settings_refresh_job is not None:
            try:
                self.after_cancel(self._settings_refresh_job)
            except Exception:
                pass
            self._settings_refresh_job = None

    def _settings_scroll_fix(self, event):
        if not (self.settings_window and self.settings_window.winfo_exists()):
            return
        container = getattr(self, "settings_container", None)
        if not container:
            return
        canvas = getattr(container, "_parent_canvas", None)
        if not canvas:
            return
        try:
            if container.check_if_master_is_canvas(event.widget):
                self._schedule_settings_canvas_refresh()
        except Exception:
            return

    def _settings_scrollbar_command(self, *args):
        original = self._settings_scrollbar_original
        if original:
            original(*args)
        self._schedule_settings_canvas_refresh()

    def _schedule_settings_canvas_refresh(self):
        if self._settings_refresh_job is not None:
            return
        self._settings_refresh_job = self.after(16, self._refresh_settings_canvas)

    def _refresh_settings_canvas(self):
        self._settings_refresh_job = None
        if not (self.settings_window and self.settings_window.winfo_exists()):
            return
        container = getattr(self, "settings_container", None)
        canvas = getattr(container, "_parent_canvas", None) if container else None
        if not canvas:
            return
        try:
            canvas.update_idletasks()
            canvas.configure(scrollregion=canvas.bbox("all"))
            canvas.yview_moveto(canvas.yview()[0])
        except Exception:
            return

    def save_delay_settings(self):
        dm_delay = self._parse_delay_range(
            self.dm_delay_min_input,
            self.dm_delay_max_input,
            "DM delay (h)",
            cast_type=float,
            min_value=0.0,
        )
        if not dm_delay:
            return
        join_delay = self._parse_delay_range(
            self.join_delay_min_input,
            self.join_delay_max_input,
            "Join delay (h)",
            cast_type=float,
            min_value=0.0,
        )
        if not join_delay:
            return
        friend_delay = self._parse_delay_range(
            self.friend_delay_min_input,
            self.friend_delay_max_input,
            "Friend request delay (h)",
            cast_type=float,
            min_value=0.0,
        )
        if not friend_delay:
            return
        if hasattr(self, "status_delay_min_input") and self.status_delay_min_input.winfo_exists():
            status_delay = self._parse_delay_range(
                self.status_delay_min_input,
                self.status_delay_max_input,
                "Status delay (hours)",
                cast_type=float,
                min_value=0.1,
            )
        else:
            status_delay = self._parse_delay_range_from_settings(
                "status_delay_min_hours",
                "status_delay_max_hours",
                "Status delay (hours)",
                cast_type=float,
                min_value=0.1,
            )
        if not status_delay:
            return
        account_interval = self._parse_min_value(
            self.account_min_interval_input,
            "Account min interval (h)",
            cast_type=float,
            min_value=0.0,
        )
        if account_interval is None:
            return
        target_interval = self._parse_min_value(
            self.target_min_interval_input,
            "Target min interval (h)",
            cast_type=float,
            min_value=0.0,
        )
        if target_interval is None:
            return
        dm_delay_min_h, dm_delay_max_h = dm_delay
        join_delay_min_h, join_delay_max_h = join_delay
        friend_delay_min_h, friend_delay_max_h = friend_delay
        status_delay_min, status_delay_max = status_delay
        self.db.set_setting("dm_delay_min", str(dm_delay_min_h))
        self.db.set_setting("dm_delay_max", str(dm_delay_max_h))
        self.db.set_setting("join_delay_min", str(join_delay_min_h))
        self.db.set_setting("join_delay_max", str(join_delay_max_h))
        self.db.set_setting("friend_delay_min", str(friend_delay_min_h))
        self.db.set_setting("friend_delay_max", str(friend_delay_max_h))
        self.db.set_setting("status_delay_min_hours", str(status_delay_min))
        self.db.set_setting("status_delay_max_hours", str(status_delay_max))
        self.db.set_setting("account_min_interval", str(account_interval))
        self.db.set_setting("target_min_interval", str(target_interval))
        self._set_setting_bool("delay_units_hours", True)
        self.add_log("[Settings] Zapisano ustawienia delay.")

    def _build_settings_sections(self, parent):
        # 1. CONFIGURATION SECTION (Accounts + Proxy)
        self.acc_frame = ctk.CTkFrame(parent)
        self.acc_frame.pack(fill="x", pady=10)
        ctk.CTkLabel(self.acc_frame, text="Account & Proxy Management", font=ctk.CTkFont(size=16, weight="bold")).grid(row=0, column=0, columnspan=2, pady=10)
        ctk.CTkLabel(self.acc_frame, text="Discord Token").grid(row=1, column=0, padx=10, pady=(0, 2), sticky="w")
        self.token_input = ctk.CTkEntry(self.acc_frame, placeholder_text="Discord Token", width=350)
        self.token_input.grid(row=2, column=0, padx=10, pady=5)
        ctk.CTkLabel(self.acc_frame, text="Proxy (http://user:pass@ip:port)").grid(row=3, column=0, padx=10, pady=(0, 2), sticky="w")
        self.proxy_input = ctk.CTkEntry(self.acc_frame, placeholder_text="Proxy (http://user:pass@ip:port)", width=350)
        self.proxy_input.grid(row=4, column=0, padx=10, pady=5)
        self.require_proxy_toggle = ctk.CTkCheckBox(
            self.acc_frame,
            text="Require proxy for all actions",
            variable=self.require_proxy_var,
        )
        self.require_proxy_toggle.grid(row=5, column=0, padx=10, pady=(0, 5), sticky="w")
        ctk.CTkLabel(self.acc_frame, text="DM daily limit").grid(row=6, column=0, padx=10, pady=(0, 2), sticky="w")
        self.dm_limit_input = ctk.CTkEntry(self.acc_frame, placeholder_text="DM daily limit (np. 15)", width=350)
        self.dm_limit_input.grid(row=7, column=0, padx=10, pady=5)
        self.dm_limit_input.insert(0, "15")
        ctk.CTkLabel(self.acc_frame, text="Join daily limit").grid(row=8, column=0, padx=10, pady=(0, 2), sticky="w")
        self.join_limit_input = ctk.CTkEntry(self.acc_frame, placeholder_text="Join daily limit (np. 5)", width=350)
        self.join_limit_input.grid(row=9, column=0, padx=10, pady=5)
        self.join_limit_input.insert(0, "5")
        self.add_acc_btn = ctk.CTkButton(self.acc_frame, text="Add Account", command=self.add_account)
        self.add_acc_btn.grid(row=2, column=1, rowspan=6, padx=10, pady=5, sticky="ns")
        self.proxy_test_btn = ctk.CTkButton(
            self.acc_frame,
            text="Test Proxy",
            fg_color="#3498db",
            command=self.test_proxy_settings,
        )
        self.proxy_test_btn.grid(row=8, column=1, padx=10, pady=5)
        self.proxy_test_all_btn = ctk.CTkButton(
            self.acc_frame,
            text="Test All Proxies",
            fg_color="#1abc9c",
            command=self.test_all_account_proxies,
        )
        self.proxy_test_all_btn.grid(row=9, column=1, padx=10, pady=(0, 5))
        ctk.CTkLabel(self.acc_frame, text="Account ID to remove").grid(row=10, column=0, padx=10, pady=(0, 2), sticky="w")
        self.remove_account_input = ctk.CTkEntry(self.acc_frame, placeholder_text="Account ID to remove", width=350)
        self.remove_account_input.grid(row=11, column=0, padx=10, pady=5)
        self.remove_account_btn = ctk.CTkButton(self.acc_frame, text="Remove Account", fg_color="#e74c3c", command=self.remove_account_by_id)
        self.remove_account_btn.grid(row=11, column=1, padx=10, pady=5)
        self.export_banlist_plaintext_toggle = ctk.CTkCheckBox(
            self.acc_frame,
            text="Export banlist tokens in plaintext (unsafe)",
            variable=self.export_banned_tokens_plaintext_var,
            command=self.on_export_plaintext_toggle,
        )
        self.export_banlist_plaintext_toggle.grid(row=12, column=0, columnspan=2, padx=10, pady=(5, 0), sticky="w")

        self.acc_overview_frame = ctk.CTkFrame(parent)
        self.acc_overview_frame.pack(fill="x", pady=10)
        ctk.CTkLabel(self.acc_overview_frame, text="Account Counters & Status", font=ctk.CTkFont(size=16, weight="bold")).grid(row=0, column=0, columnspan=2, pady=10)
        self.acc_status_legend = ctk.CTkFrame(self.acc_overview_frame, fg_color="transparent")
        self.acc_status_legend.grid(row=1, column=0, columnspan=2, padx=10, pady=(0, 5), sticky="w")
        ctk.CTkLabel(self.acc_status_legend, text="Active", text_color=self.account_status_colors["active"]).pack(side="left", padx=(0, 12))
        ctk.CTkLabel(self.acc_status_legend, text="Unverified", text_color=self.account_status_colors["unverified"]).pack(side="left", padx=(0, 12))
        ctk.CTkLabel(self.acc_status_legend, text="Banned/Dead", text_color=self.account_status_colors["banned"]).pack(side="left")
        self.acc_overview_box = ctk.CTkTextbox(self.acc_overview_frame, height=120)
        self.acc_overview_box.grid(row=2, column=0, padx=10, pady=5, sticky="ew")
        self.acc_overview_frame.grid_columnconfigure(0, weight=1)
        self.acc_refresh_btn = ctk.CTkButton(self.acc_overview_frame, text="Refresh Accounts", command=self.refresh_accounts_overview)
        self.acc_refresh_btn.grid(row=2, column=1, padx=10, pady=5)
        self.acc_reset_btn = ctk.CTkButton(self.acc_overview_frame, text="Reset Counters", fg_color="#f39c12", hover_color="#d35400", command=self.reset_account_counters)
        self.acc_reset_btn.grid(row=3, column=1, padx=10, pady=5)

        self.profile_frame = ctk.CTkFrame(parent)
        self.profile_frame.pack(fill="x", pady=10)
        ctk.CTkLabel(self.profile_frame, text="Profile (Name & Avatar)", font=ctk.CTkFont(size=16, weight="bold")).grid(
            row=0, column=0, columnspan=3, pady=10
        )
        ctk.CTkLabel(self.profile_frame, text="Base name").grid(row=1, column=0, padx=10, pady=(0, 2), sticky="w")
        self.profile_name_input = ctk.CTkEntry(self.profile_frame, placeholder_text="Base name", width=260)
        self.profile_name_input.grid(row=2, column=0, padx=10, pady=5, sticky="w")
        self.profile_change_name_toggle = ctk.CTkCheckBox(
            self.profile_frame,
            text="Change name",
            variable=self.profile_change_name_var,
        )
        self.profile_change_name_toggle.grid(row=2, column=1, padx=10, pady=5, sticky="w")
        self.profile_append_suffix_toggle = ctk.CTkCheckBox(
            self.profile_frame,
            text="Add unique suffix",
            variable=self.profile_append_suffix_var,
        )
        self.profile_append_suffix_toggle.grid(row=2, column=2, padx=10, pady=5, sticky="w")

        ctk.CTkLabel(self.profile_frame, text="Avatar image file").grid(row=3, column=0, padx=10, pady=(0, 2), sticky="w")
        self.profile_avatar_input = ctk.CTkEntry(self.profile_frame, placeholder_text="Avatar file path", width=260)
        self.profile_avatar_input.grid(row=4, column=0, padx=10, pady=5, sticky="w")
        self.profile_avatar_browse_btn = ctk.CTkButton(
            self.profile_frame,
            text="Browse",
            width=100,
            command=self.browse_avatar_file,
        )
        self.profile_avatar_browse_btn.grid(row=4, column=1, padx=10, pady=5, sticky="w")
        self.profile_change_avatar_toggle = ctk.CTkCheckBox(
            self.profile_frame,
            text="Change avatar",
            variable=self.profile_change_avatar_var,
        )
        self.profile_change_avatar_toggle.grid(row=4, column=2, padx=10, pady=5, sticky="w")
        self.profile_save_btn = ctk.CTkButton(
            self.profile_frame,
            text="Save Profile",
            command=self.save_profile_settings,
        )
        self.profile_save_btn.grid(row=5, column=0, padx=10, pady=(5, 10), sticky="w")
        self.profile_apply_btn = ctk.CTkButton(
            self.profile_frame,
            text="Apply to Accounts",
            fg_color="#1abc9c",
            command=self.apply_profile_settings,
        )
        self.profile_apply_btn.grid(row=5, column=1, padx=10, pady=(5, 10), sticky="w")
        self._load_profile_settings()

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
        info_row = ctk.CTkFrame(self.msg_frame, fg_color="transparent")
        info_row.pack(fill="x", padx=20, pady=(0, 5))
        ctk.CTkLabel(
            info_row,
            text="One tab = one template. Tokens: [[tag]], [[emoji]], [[num]], [[num:1-99]] | Spintax: {a|b}",
            font=ctk.CTkFont(size=12),
            text_color="#b0b0b0",
            anchor="w",
        ).pack(side="left", fill="x", expand=True)
        ctk.CTkButton(
            info_row,
            text="?",
            width=28,
            height=24,
            command=self._show_template_help,
        ).pack(side="right")
        self.template_tabs = ctk.CTkTabview(self.msg_frame, command=self._on_template_tab_changed)
        self.template_tabs.pack(fill="x", padx=20, pady=10)
        self.template_boxes = []
        self.template_count_labels = []
        for index in range(1, 11):
            tab = self.template_tabs.add(f"Template {index}")
            box = ctk.CTkTextbox(tab, height=120)
            box.pack(fill="x", padx=10, pady=(10, 6))
            count_label = ctk.CTkLabel(tab, text=f"Chars: 0/{self._template_max_chars}", text_color="#8a8a8a")
            count_label.pack(anchor="e", padx=10, pady=(0, 8))
            box.bind("<<Modified>>", lambda event, idx=index - 1, ref=box: self._on_template_modified(idx, ref))
            self.template_boxes.append(box)
            self.template_count_labels.append(count_label)
        self.friend_request_toggle = ctk.CTkCheckBox(
            self.msg_frame,
            text="Send friend request before DM",
            variable=self.friend_request_var,
        )
        self.friend_request_toggle.pack(anchor="w", padx=20, pady=(0, 10))
        self.dry_run_toggle = ctk.CTkCheckBox(
            self.msg_frame,
            text="Dry-run (log only, no send)",
            variable=self.dry_run_var,
        )
        self.dry_run_toggle.pack(anchor="w", padx=20, pady=(0, 10))
        self.templates_action_row = ctk.CTkFrame(self.msg_frame, fg_color="transparent")
        self.templates_action_row.pack(fill="x", padx=20, pady=(0, 10))
        self.templates_save_btn = ctk.CTkButton(
            self.templates_action_row,
            text="Save Templates",
            command=self.save_message_templates,
        )
        self.templates_save_btn.pack(side="left")
        self.template_save_status_label = ctk.CTkLabel(
            self.templates_action_row,
            text="Save status: idle",
            text_color="#8a8a8a",
        )
        self.template_save_status_label.pack(side="right")
        self._load_message_templates()

        self.delay_frame = ctk.CTkFrame(parent)
        self.delay_frame.pack(fill="x", pady=10)
        ctk.CTkLabel(self.delay_frame, text="Delay Settings", font=ctk.CTkFont(size=16, weight="bold")).grid(row=0, column=0, columnspan=4, pady=10)
        ctk.CTkLabel(self.delay_frame, text="Min", text_color="#8a8a8a").grid(row=1, column=1, padx=10, pady=(0, 5), sticky="w")
        ctk.CTkLabel(self.delay_frame, text="Max", text_color="#8a8a8a").grid(row=1, column=2, padx=10, pady=(0, 5), sticky="w")

        dm_delay_min = self._convert_delay_value(
            self._get_setting_number("dm_delay_min", None),
            assume_seconds=True,
        )
        if dm_delay_min is None:
            dm_delay_min = self._seconds_to_hours(5)
        dm_delay_max = self._convert_delay_value(
            self._get_setting_number("dm_delay_max", None),
            assume_seconds=True,
        )
        if dm_delay_max is None:
            dm_delay_max = self._seconds_to_hours(10)
        join_delay_min = self._convert_delay_value(
            self._get_setting_number("join_delay_min", None),
            assume_seconds=True,
        )
        if join_delay_min is None:
            join_delay_min = self._seconds_to_hours(10)
        join_delay_max = self._convert_delay_value(
            self._get_setting_number("join_delay_max", None),
            assume_seconds=True,
        )
        if join_delay_max is None:
            join_delay_max = self._seconds_to_hours(30)
        friend_delay_min = self._convert_delay_value(
            self._get_setting_number("friend_delay_min", None),
            assume_seconds=True,
        )
        if friend_delay_min is None:
            friend_delay_min = self._seconds_to_hours(2)
        friend_delay_max = self._convert_delay_value(
            self._get_setting_number("friend_delay_max", None),
            assume_seconds=True,
        )
        if friend_delay_max is None:
            friend_delay_max = self._seconds_to_hours(5)
        status_delay_min = self._convert_delay_value(
            self._get_setting_number("status_delay_min_hours", 3.0),
            assume_seconds=False,
        )
        status_delay_max = self._convert_delay_value(
            self._get_setting_number("status_delay_max_hours", 3.0),
            assume_seconds=False,
        )
        account_min_interval = self._convert_delay_value(
            self._get_setting_number("account_min_interval", 0),
            assume_seconds=True,
        )
        target_min_interval = self._convert_delay_value(
            self._get_setting_number("target_min_interval", 0),
            assume_seconds=True,
        )

        ctk.CTkLabel(self.delay_frame, text="DM delay (h) min/max").grid(row=2, column=0, padx=10, pady=5, sticky="w")
        self.dm_delay_min_input = ctk.CTkEntry(self.delay_frame, width=120)
        self.dm_delay_min_input.grid(row=2, column=1, padx=10, pady=5, sticky="w")
        self.dm_delay_min_input.insert(0, self._format_hours_value(dm_delay_min))
        self.dm_delay_max_input = ctk.CTkEntry(self.delay_frame, width=120)
        self.dm_delay_max_input.grid(row=2, column=2, padx=10, pady=5, sticky="w")
        self.dm_delay_max_input.insert(0, self._format_hours_value(dm_delay_max))

        ctk.CTkLabel(self.delay_frame, text="Join delay (h) min/max").grid(row=3, column=0, padx=10, pady=5, sticky="w")
        self.join_delay_min_input = ctk.CTkEntry(self.delay_frame, width=120)
        self.join_delay_min_input.grid(row=3, column=1, padx=10, pady=5, sticky="w")
        self.join_delay_min_input.insert(0, self._format_hours_value(join_delay_min))
        self.join_delay_max_input = ctk.CTkEntry(self.delay_frame, width=120)
        self.join_delay_max_input.grid(row=3, column=2, padx=10, pady=5, sticky="w")
        self.join_delay_max_input.insert(0, self._format_hours_value(join_delay_max))

        ctk.CTkLabel(self.delay_frame, text="Friend request delay (h) min/max").grid(row=4, column=0, padx=10, pady=5, sticky="w")
        self.friend_delay_min_input = ctk.CTkEntry(self.delay_frame, width=120)
        self.friend_delay_min_input.grid(row=4, column=1, padx=10, pady=5, sticky="w")
        self.friend_delay_min_input.insert(0, self._format_hours_value(friend_delay_min))
        self.friend_delay_max_input = ctk.CTkEntry(self.delay_frame, width=120)
        self.friend_delay_max_input.grid(row=4, column=2, padx=10, pady=5, sticky="w")
        self.friend_delay_max_input.insert(0, self._format_hours_value(friend_delay_max))

        ctk.CTkLabel(self.delay_frame, text="Status delay (h) min/max").grid(row=5, column=0, padx=10, pady=5, sticky="w")
        self.status_delay_min_input = ctk.CTkEntry(self.delay_frame, width=120)
        self.status_delay_min_input.grid(row=5, column=1, padx=10, pady=5, sticky="w")
        self.status_delay_min_input.insert(0, self._format_hours_value(status_delay_min))
        self.status_delay_max_input = ctk.CTkEntry(self.delay_frame, width=120)
        self.status_delay_max_input.grid(row=5, column=2, padx=10, pady=5, sticky="w")
        self.status_delay_max_input.insert(0, self._format_hours_value(status_delay_max))

        ctk.CTkLabel(self.delay_frame, text="Account min interval (h)").grid(row=6, column=0, padx=10, pady=5, sticky="w")
        self.account_min_interval_input = ctk.CTkEntry(self.delay_frame, width=120)
        self.account_min_interval_input.grid(row=6, column=1, padx=10, pady=5, sticky="w")
        self.account_min_interval_input.insert(0, self._format_hours_value(account_min_interval))

        ctk.CTkLabel(self.delay_frame, text="Target min interval (h)").grid(row=7, column=0, padx=10, pady=5, sticky="w")
        self.target_min_interval_input = ctk.CTkEntry(self.delay_frame, width=120)
        self.target_min_interval_input.grid(row=7, column=1, padx=10, pady=5, sticky="w")
        self.target_min_interval_input.insert(0, self._format_hours_value(target_min_interval))

        self.delay_save_btn = ctk.CTkButton(self.delay_frame, text="Save Delays", command=self.save_delay_settings)
        self.delay_save_btn.grid(row=8, column=0, padx=10, pady=10, sticky="w")

        # 2. JOINER SECTION (NEW)
        self.joiner_frame = ctk.CTkFrame(parent)
        self.joiner_frame.pack(fill="x", pady=10)
        ctk.CTkLabel(self.joiner_frame, text="Server Joiner (Mass Join)", font=ctk.CTkFont(size=16, weight="bold")).grid(row=0, column=0, columnspan=2, pady=10)
        ctk.CTkLabel(self.joiner_frame, text="Invite links/codes (one per line)").grid(row=1, column=0, columnspan=2, padx=10, pady=(0, 2), sticky="w")
        self.invite_input = ctk.CTkTextbox(self.joiner_frame, height=80, width=350)
        self.invite_input.grid(row=2, column=0, padx=10, pady=5)
        self.invite_input.insert("1.0", "Invite link/code per line (e.g. discord.gg/xyz)\n")
        self.invite_save_btn = ctk.CTkButton(
            self.joiner_frame,
            text="Save Servers",
            fg_color="#3498db",
            command=self.save_invite_settings,
        )
        self.invite_save_btn.grid(row=2, column=1, padx=10, pady=5)
        self.joiner_rules_toggle = ctk.CTkCheckBox(
            self.joiner_frame,
            text="Accept rules after join",
            variable=self.auto_accept_rules_var,
        )
        self.joiner_rules_toggle.grid(row=3, column=0, padx=10, pady=5, sticky="w")
        self.joiner_onboarding_toggle = ctk.CTkCheckBox(
            self.joiner_frame,
            text="Complete onboarding (select all roles)",
            variable=self.auto_onboarding_var,
        )
        self.joiner_onboarding_toggle.grid(row=4, column=0, padx=10, pady=(0, 10), sticky="w")
        ctk.CTkLabel(
            self.joiner_frame,
            text="Role whitelist (IDs, one per line)",
        ).grid(row=5, column=0, padx=10, pady=(0, 2), sticky="w")
        self.onboarding_role_whitelist_input = ctk.CTkTextbox(self.joiner_frame, height=80, width=350)
        self.onboarding_role_whitelist_input.grid(row=6, column=0, padx=10, pady=5, sticky="w")
        self.joiner_onboarding_hint = ctk.CTkLabel(
            self.joiner_frame,
            text="If set, onboarding selects matching roles, otherwise falls back to first options.",
            text_color="#8a8a8a",
        )
        self.joiner_onboarding_hint.grid(row=7, column=0, padx=10, pady=(0, 10), sticky="w")
        self._load_invite_settings()
        self._load_joiner_settings()

        # 3. CAPTCHA SECTION
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
        ctk.CTkLabel(self.captcha_frame, text="Provider").grid(row=1, column=0, padx=10, pady=(0, 2), sticky="w")
        ctk.CTkLabel(self.captcha_frame, text="API Key").grid(row=1, column=1, padx=10, pady=(0, 2), sticky="w")
        self.captcha_provider = ctk.CTkOptionMenu(
            self.captcha_frame,
            values=list(self.captcha_provider_labels.keys()),
            variable=self.captcha_provider_var,
            command=self.on_captcha_provider_change,
        )
        self.captcha_provider.grid(row=2, column=0, padx=10, pady=5)
        self.captcha_key_input = ctk.CTkEntry(self.captcha_frame, placeholder_text="API Key", width=350)
        self.captcha_key_input.grid(row=2, column=1, padx=10, pady=5)
        self.captcha_save_btn = ctk.CTkButton(self.captcha_frame, text="Save", command=self.save_captcha_settings)
        self.captcha_save_btn.grid(row=2, column=2, padx=10, pady=5)
        self.captcha_test_btn = ctk.CTkButton(self.captcha_frame, text="Test API", fg_color="#16a085", command=self.test_captcha_settings)
        self.captcha_test_btn.grid(row=3, column=1, padx=10, pady=5)
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
        self.target_import_btn = ctk.CTkButton(self.target_frame, text="Import from file (.txt)", command=self.import_targets_from_file)
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
        ctk.CTkLabel(self.status_frame, text="Custom Status").grid(row=1, column=0, padx=10, pady=(0, 2), sticky="w")
        ctk.CTkLabel(self.status_frame, text="Status Type").grid(row=1, column=1, padx=10, pady=(0, 2), sticky="w")
        self.status_text_input = ctk.CTkEntry(self.status_frame, placeholder_text="Custom Status", width=350)
        self.status_text_input.grid(row=2, column=0, padx=10, pady=5)
        stored_status_text = self.db.get_setting("status_text", "").strip()
        self.status_text_input.insert(0, stored_status_text or "Playing Metin2")
        stored_status_type = self.db.get_setting("status_type", "online").strip() or "online"
        self.status_type_var = ctk.StringVar(value=stored_status_type)
        self.status_dropdown = ctk.CTkOptionMenu(self.status_frame, values=["online", "idle", "dnd", "invisible"], variable=self.status_type_var)
        self.status_dropdown.grid(row=2, column=1, padx=10, pady=5)
        self.update_status_btn = ctk.CTkButton(self.status_frame, text="Start Auto Status", fg_color="#9b59b6", command=self.start_status_update)
        self.update_status_btn.grid(row=3, column=0, pady=10, sticky="w")
        self.stop_status_btn = ctk.CTkButton(self.status_frame, text="Stop Auto Status", fg_color="#c0392b", command=self.stop_status_update)
        self.stop_status_btn.grid(row=3, column=1, pady=10, sticky="e")
        self.status_state_label = ctk.CTkLabel(
            self.status_frame,
            text="Auto status: stopped",
            text_color="#8a8a8a",
            anchor="w",
        )
        self.status_state_label.grid(row=4, column=0, columnspan=2, padx=10, pady=(0, 10), sticky="w")

        # 5. SEKCJA SCRAPERA
        self.scrape_frame = ctk.CTkFrame(parent)
        self.scrape_frame.pack(fill="x", pady=10)
        ctk.CTkLabel(self.scrape_frame, text="Scraping Tools", font=ctk.CTkFont(size=16, weight="bold")).grid(row=0, column=0, columnspan=3, pady=10)
        ctk.CTkLabel(self.scrape_frame, text="Channel ID").grid(row=1, column=0, padx=10, pady=(0, 2), sticky="w")
        ctk.CTkLabel(self.scrape_frame, text="Range (limit)").grid(row=1, column=1, padx=10, pady=(0, 2), sticky="w")
        self.scrape_channel_input = ctk.CTkEntry(self.scrape_frame, placeholder_text="Channel ID", width=220)
        self.scrape_channel_input.grid(row=2, column=0, padx=10, pady=5, sticky="w")
        self.scrape_range_input = ctk.CTkEntry(self.scrape_frame, placeholder_text="Range (limit)", width=140)
        self.scrape_range_input.grid(row=2, column=1, padx=10, pady=5, sticky="w")
        self.scrape_btn = ctk.CTkButton(self.scrape_frame, text="Scrape Users", command=self.start_scraping)
        self.scrape_btn.grid(row=2, column=2, padx=10, pady=5)
        ctk.CTkLabel(self.scrape_frame, text="Scrape proxy").grid(row=3, column=0, padx=10, pady=(0, 2), sticky="w")
        self.scrape_proxy_input = ctk.CTkEntry(self.scrape_frame, placeholder_text="http://user:pass@ip:port", width=350)
        self.scrape_proxy_input.grid(row=4, column=0, padx=10, pady=5, columnspan=2, sticky="w")
        ctk.CTkLabel(self.scrape_frame, text="Guild ID").grid(row=5, column=0, padx=10, pady=(0, 2), sticky="w")
        self.scrape_guild_input = ctk.CTkEntry(self.scrape_frame, placeholder_text="Guild ID", width=350)
        self.scrape_guild_input.grid(row=6, column=0, padx=10, pady=5, columnspan=2, sticky="w")
        self.scrape_guild_btn = ctk.CTkButton(
            self.scrape_frame,
            text="Scrape Guild Members",
            fg_color="#16a085",
            command=self.start_guild_scraping,
        )
        self.scrape_guild_btn.grid(row=6, column=2, padx=10, pady=5, sticky="w")
        self._load_scrape_settings()
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
            self.add_log(f"[Captcha] Error ({provider}) - {msg}")

if __name__ == "__main__":
    app = MassDMApp()
    app.mainloop()















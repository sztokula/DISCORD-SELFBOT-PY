import customtkinter as ctk
import queue
import re
import threading
from datetime import datetime
from tkinter import filedialog
from urllib.parse import urlparse
from database import DatabaseManager
from captcha_solver import CaptchaSolver
from discord_worker import DiscordWorker
from scraper import DiscordScraper
from status_changer import StatusChanger
from joiner import DiscordJoiner # Import nowego modułu
from token_manager import TokenManager

ctk.set_appearance_mode("Dark")
ctk.set_default_color_theme("blue")

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
        self.log_queue = queue.Queue()
        self.worker = DiscordWorker(self.db, self.add_log)
        self.scraper = DiscordScraper(self.db, self.add_log)
        self.status_changer = StatusChanger(self.db, self.add_log)
        self.captcha_solver = CaptchaSolver(self.db, self.add_log)
        self.joiner = DiscordJoiner(self.db, self.add_log, self.captcha_solver) # Inicjalizacja
        self.token_manager = TokenManager(self.db, self.add_log)
        self.log_entries = []
        self.log_filter_var = ctk.StringVar()
        self.logs_dir = Path("logs")
        self.logs_dir.mkdir(parents=True, exist_ok=True)
        self.log_file_path = self.logs_dir / f"app_{datetime.now().strftime('%Y%m%d')}.log"

        self.title("Mass-DM Farm Tool Pro v1.0")
        self.geometry("1100x1000") # Zwiększona wysokość na nową sekcję

        self.grid_columnconfigure(1, weight=1)
        self.grid_rowconfigure(0, weight=1)
        self.settings_window = None

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

        # 1. SEKCJA WIADOMOŚCI
        self.msg_frame = ctk.CTkFrame(self.main_container)
        self.msg_frame.pack(fill="x", pady=10)
        ctk.CTkLabel(self.msg_frame, text="Message Templates", font=ctk.CTkFont(size=16, weight="bold")).pack(pady=5)
        ctk.CTkLabel(
            self.msg_frame,
            text="Szablony oddzielaj linią: --- | Tokeny: [[tag]], [[emoji]], [[num]], [[num:1-99]] | Spintax: {a|b}",
            font=ctk.CTkFont(size=12),
            text_color="#b0b0b0",
        ).pack(pady=(0, 5))
        self.msg_input = ctk.CTkTextbox(self.msg_frame, height=100)
        self.msg_input.pack(fill="x", padx=20, pady=10)
        self.friend_request_var = ctk.BooleanVar(value=False)
        self.friend_request_toggle = ctk.CTkCheckBox(
            self.msg_frame,
            text="Wyślij zaproszenie do znajomych przed DM",
            variable=self.friend_request_var,
        )
        self.friend_request_toggle.pack(anchor="w", padx=20, pady=(0, 10))

        # 2. SEKCJA LOGÓW
        self.log_frame = ctk.CTkFrame(self.main_container)
        self.log_frame.pack(fill="both", expand=True, pady=10)
        self.log_controls = ctk.CTkFrame(self.log_frame, fg_color="transparent")
        self.log_controls.pack(fill="x", padx=10, pady=(10, 0))
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
        self.log_box = ctk.CTkTextbox(self.log_frame, height=200, fg_color="#1a1a1a")
        self.log_box.pack(fill="both", padx=10, pady=10)

        # --- BOTTOM CONTROL BAR ---
        self.control_bar = ctk.CTkFrame(self, height=80)
        self.control_bar.grid(row=1, column=1, sticky="ew", padx=20, pady=(0, 20))
        self.start_btn = ctk.CTkButton(self.control_bar, text="START MISSION", fg_color="#2ecc71", command=self.start_mission)
        self.start_btn.pack(side="left", padx=50, pady=20)
        self.stop_btn = ctk.CTkButton(self.control_bar, text="STOP ALL", fg_color="#e74c3c", command=self.stop_all)
        self.stop_btn.pack(side="right", padx=50, pady=20)

        self.after(100, self.process_log_queue)
        self.open_settings_window()

    def add_log(self, message):
        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.log_queue.put({"timestamp": timestamp, "message": message})

    def log_error(self, message):
        self.add_log(f"Błąd: {message}")

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
            self.log_error(f"{label} (min) musi być liczbą.")
            return None
        try:
            max_val = cast_type(max_raw)
        except (TypeError, ValueError):
            self.log_error(f"{label} (max) musi być liczbą.")
            return None
        if min_val < min_value or max_val < min_value:
            self.log_error(f"{label} musi być >= {min_value}.")
            return None
        if min_val > max_val:
            self.log_error(f"{label} min nie może być większy od max.")
            return None
        return min_val, max_val

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

    def is_valid_guild_id(self, guild_id):
        if not guild_id:
            self.log_error("Niepoprawny Guild ID: puste pole.")
            return False
        if not re.match(r"^\d{17,20}$", guild_id):
            self.log_error("Niepoprawny format Guild ID.")
            return False
        return True

    def process_log_queue(self):
        try:
            while True:
                entry = self.log_queue.get_nowait()
                self.log_entries.append(entry)
                self._write_log_to_file(entry)
                if self._matches_log_filter(entry):
                    self._append_log_entry(entry)
        except queue.Empty:
            pass
        self.after(100, self.process_log_queue)

    def _append_log_entry(self, entry):
        self.log_box.insert("end", f"[{entry['timestamp']}] {entry['message']}\n")
        self.log_box.see("end")

    def _matches_log_filter(self, entry):
        filter_text = self.log_filter_var.get().strip().lower()
        if not filter_text:
            return True
        return filter_text in entry["message"].lower() or filter_text in entry["timestamp"].lower()

    def apply_log_filter(self, *_args):
        self.log_box.delete("1.0", "end")
        for entry in self.log_entries:
            if self._matches_log_filter(entry):
                self._append_log_entry(entry)

    def _write_log_to_file(self, entry):
        try:
            with self.log_file_path.open("a", encoding="utf-8") as log_file:
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
            self.log_error("Limit DM musi być liczbą całkowitą.")
            return
        try:
            join_limit = int(join_limit_raw) if join_limit_raw else 5
        except ValueError:
            self.log_error("Limit joinów musi być liczbą całkowitą.")
            return
        if dm_limit <= 0 or join_limit <= 0:
            self.log_error("Limity muszą być większe od zera.")
            return
        is_valid, info = self.token_manager.validate_token(token)
        if not is_valid:
            self.log_error(f"Token niepoprawny: {info}.")
            return
        if self.db.add_account("discord", token, proxy, dm_limit, join_limit):
            self.add_log(f"Account added: {info}. DM limit: {dm_limit}, Join limit: {join_limit}.")
            self.token_input.delete(0, 'end')
            self.proxy_input.delete(0, 'end')
            self.dm_limit_input.delete(0, 'end')
            self.dm_limit_input.insert(0, "15")
            self.join_limit_input.delete(0, 'end')
            self.join_limit_input.insert(0, "5")
        else:
            self.log_error("Konto już istnieje lub token jest niepoprawny.")

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

    def start_joining(self):
        if not self.module_vars["joiner"].get():
            self.log_error("Moduł Joiner jest wyłączony.")
            return
        invites = self._get_invite_list()
        if not invites:
            self.log_error("Brak poprawnych zaproszeń.")
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
        join_delay_min, join_delay_max = join_delay
        self.db.set_setting("join_delay_min", str(join_delay_min))
        self.db.set_setting("join_delay_max", str(join_delay_max))
        thread = threading.Thread(target=self.joiner.run_mass_join, args=(invites, join_delay_min, join_delay_max))
        thread.daemon = True
        thread.start()

    def start_scraping(self):
        if not self.module_vars["scraper"].get():
            self.log_error("Moduł Scraper jest wyłączony.")
            return
        token = self.token_input.get().strip()
        channel_id = self.scrape_channel_input.get().strip()
        if not self.validate_token_format(token):
            return
        if not self.is_valid_channel_id(channel_id):
            return
        thread = threading.Thread(target=self.scraper.scrape_history, args=(token, channel_id, 500))
        thread.daemon = True
        thread.start()

    def start_guild_scraping(self):
        if not self.module_vars["scraper"].get():
            self.log_error("Moduł Scraper jest wyłączony.")
            return
        token = self.token_input.get().strip()
        guild_id = self.scrape_guild_input.get().strip()
        if not self.validate_token_format(token):
            return
        if not self.is_valid_guild_id(guild_id):
            return
        thread = threading.Thread(target=self.scraper.scrape_guild_members, args=(token, guild_id, 1000))
        thread.daemon = True
        thread.start()

    def start_status_update(self):
        if not self.module_vars["status"].get():
            self.log_error("Moduł Status jest wyłączony.")
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
            self.log_error("Moduł DM jest wyłączony.")
            return
        raw = self.msg_input.get("1.0", "end").strip()
        if not raw:
            self.log_error("Pusta wiadomość.")
            return
        templates = [tpl.strip() for tpl in re.split(r"\n-{3,}\n", raw) if tpl.strip()]
        if not templates:
            self.log_error("Brak poprawnych szablonów wiadomości.")
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
        dm_delay_min, dm_delay_max = dm_delay
        friend_delay_min, friend_delay_max = friend_delay
        self.db.set_setting("dm_delay_min", str(dm_delay_min))
        self.db.set_setting("dm_delay_max", str(dm_delay_max))
        self.db.set_setting("friend_delay_min", str(friend_delay_min))
        self.db.set_setting("friend_delay_max", str(friend_delay_max))
        use_friend_req = self.friend_request_var.get()
        thread = threading.Thread(
            target=self.worker.run_mission,
            args=(templates, dm_delay_min, dm_delay_max, use_friend_req, friend_delay_min, friend_delay_max),
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
            self.log_error("Podaj ID konta do usunięcia.")
            return
        try:
            account_id = int(raw_id)
        except ValueError:
            self.log_error("ID konta musi być liczbą.")
            return
        self.db.remove_account(account_id)
        self.add_log(f"[Accounts] Usunięto konto {account_id}.")
        self.remove_account_input.delete(0, "end")
        self.refresh_accounts_overview()

    def refresh_accounts_overview(self):
        accounts = self.db.get_accounts_overview()
        self.acc_overview_box.delete("1.0", "end")
        if not accounts:
            self.acc_overview_box.insert("end", "Brak kont w bazie.\n")
            return
        self.acc_overview_box.insert("end", "ID | Status | DM sent/limit | Join sent/limit | Proxy\n")
        self.acc_overview_box.insert("end", "-" * 70 + "\n")
        for acc_id, status, proxy, dm_limit, sent_today, join_limit, join_today in accounts:
            proxy_value = proxy if proxy else "-"
            line = f"{acc_id} | {status} | {sent_today}/{dm_limit} | {join_today}/{join_limit} | {proxy_value}\n"
            self.acc_overview_box.insert("end", line)

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
            self.log_error(f"Nie można odczytać pliku: {exc}")
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
        self.add_log(f"[Targets] Dodano {len(ids)} celów.")
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
        self.add_log(f"[Targets] Zaimportowano {len(ids)} celów z pliku.")
        self.refresh_targets_overview()

    def clear_targets(self):
        self.db.clear_targets()
        self.add_log("[Targets] Lista celów wyczyszczona.")
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
            self.target_overview_box.insert("end", "Brak celów w bazie.\n")
            return
        for user_id, status in targets:
            self.target_overview_box.insert("end", f"{user_id} | {status}\n")

    def on_module_toggle(self):
        self._set_setting_bool("module_dm", self.module_vars["dm"].get())
        self._set_setting_bool("module_joiner", self.module_vars["joiner"].get())
        self._set_setting_bool("module_scraper", self.module_vars["scraper"].get())
        self._set_setting_bool("module_status", self.module_vars["status"].get())
        self._set_setting_bool("module_captcha", self.module_vars["captcha"].get())
        self.apply_module_states()

    def apply_module_states(self):
        dm_enabled = self.module_vars["dm"].get()
        joiner_enabled = self.module_vars["joiner"].get()
        scraper_enabled = self.module_vars["scraper"].get()
        status_enabled = self.module_vars["status"].get()
        captcha_enabled = self.module_vars["captcha"].get()

        self.start_btn.configure(state="normal" if dm_enabled else "disabled")
        self.friend_request_toggle.configure(state="normal" if dm_enabled else "disabled")
        self.join_btn.configure(state="normal" if joiner_enabled else "disabled")
        self.scrape_btn.configure(state="normal" if scraper_enabled else "disabled")
        self.scrape_guild_btn.configure(state="normal" if scraper_enabled else "disabled")
        self.update_status_btn.configure(state="normal" if status_enabled else "disabled")
        self.stop_status_btn.configure(state="normal" if status_enabled else "disabled")
        self.captcha_save_btn.configure(state="normal" if captcha_enabled else "disabled")
        self.captcha_test_btn.configure(state="normal" if captcha_enabled else "disabled")
        self.captcha_provider.configure(state="normal" if captcha_enabled else "disabled")
        self.captcha_key_input.configure(state="normal" if captcha_enabled else "disabled")

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
        self.settings_window.protocol("WM_DELETE_WINDOW", self.close_settings_window)

    def close_settings_window(self):
        if self.settings_window and self.settings_window.winfo_exists():
            self.settings_window.destroy()
        self.settings_window = None

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

        # 2. SEKCJA JOINERA (NOWOŚĆ)
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
        self.scrape_server_input = ctk.CTkEntry(self.scrape_frame, placeholder_text="Server", width=180)
        self.scrape_server_input.grid(row=1, column=1, padx=10, pady=5, sticky="w")
        self.scrape_server_id_input = ctk.CTkEntry(self.scrape_frame, placeholder_text="Server ID", width=180)
        self.scrape_server_id_input.grid(row=1, column=2, padx=10, pady=5, sticky="w")
        self.scrape_range_input = ctk.CTkEntry(self.scrape_frame, placeholder_text="Range", width=140)
        self.scrape_range_input.grid(row=1, column=3, padx=10, pady=5, sticky="w")
        self.scrape_btn = ctk.CTkButton(self.scrape_frame, text="Scrape Users", command=self.start_scraping)
        self.scrape_btn.grid(row=1, column=4, padx=10, pady=5)
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
        self.apply_module_states()

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
            self.add_log(f"[Captcha] Błąd ({provider}) - {msg}")

if __name__ == "__main__":
    app = MassDMApp()
    app.mainloop()

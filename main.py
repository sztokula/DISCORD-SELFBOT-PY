import customtkinter as ctk
import threading
from database import DatabaseManager
from discord_worker import DiscordWorker
from scraper import DiscordScraper
from status_changer import StatusChanger
from joiner import DiscordJoiner
from token_manager import TokenManager # Import nowego modułu

ctk.set_appearance_mode("Dark")
ctk.set_default_color_theme("blue")

class MassDMApp(ctk.CTk):
    def __init__(self):
        super().__init__()
        
        self.db = DatabaseManager()
        self.worker = DiscordWorker(self.db, self.add_log)
        self.scraper = DiscordScraper(self.db, self.add_log)
        self.status_changer = StatusChanger(self.db, self.add_log)
        self.joiner = DiscordJoiner(self.db, self.add_log)
        self.token_mgr = TokenManager(self.db, self.add_log) # Inicjalizacja

        self.title("Mass-DM Farm Tool Pro v1.0")
        self.geometry("1100x1000")

        self.grid_columnconfigure(1, weight=1)
        self.grid_rowconfigure(0, weight=1)

        # --- SIDEBAR ---
        self.sidebar = ctk.CTkFrame(self, width=200, corner_radius=0)
        self.sidebar.grid(row=0, column=0, sticky="nsew")
        self.logo = ctk.CTkLabel(self.sidebar, text="FARM TOOL", font=ctk.CTkFont(size=24, weight="bold"))
        self.logo.grid(row=0, column=0, padx=20, pady=30)
        
        self.check_tokens_btn = ctk.CTkButton(self.sidebar, text="Check Tokens", fg_color="#e67e22", command=self.run_token_check)
        self.check_tokens_btn.grid(row=1, column=0, padx=20, pady=10)

        # --- MAIN CONTENT AREA ---
        self.main_container = ctk.CTkScrollableFrame(self, fg_color="transparent")
        self.main_container.grid(row=0, column=1, padx=20, pady=20, sticky="nsew")

        # 1. SEKCA KONFIGURACJI
        self.acc_frame = ctk.CTkFrame(self.main_container)
        self.acc_frame.pack(fill="x", pady=10)
        ctk.CTkLabel(self.acc_frame, text="Account & Proxy", font=ctk.CTkFont(size=16, weight="bold")).grid(row=0, column=0, columnspan=2, pady=10)
        self.token_input = ctk.CTkEntry(self.acc_frame, placeholder_text="Token", width=350)
        self.token_input.grid(row=1, column=0, padx=10, pady=5)
        self.proxy_input = ctk.CTkEntry(self.acc_frame, placeholder_text="Proxy", width=350)
        self.proxy_input.grid(row=2, column=0, padx=10, pady=5)
        self.add_acc_btn = ctk.CTkButton(self.acc_frame, text="Add Account", command=self.add_account)
        self.add_acc_btn.grid(row=1, column=1, rowspan=2, padx=10, pady=5, sticky="ns")

        # 2. SEKCJA WIADOMOŚCI (Z obsługą Spintax)
        self.msg_frame = ctk.CTkFrame(self.main_container)
        self.msg_frame.pack(fill="x", pady=10)
        ctk.CTkLabel(self.msg_frame, text="Message Template (Supports Spintax: {Hi|Hello})", font=ctk.CTkFont(size=16, weight="bold")).pack(pady=5)
        self.msg_input = ctk.CTkTextbox(self.msg_frame, height=120)
        self.msg_input.pack(fill="x", padx=20, pady=10)
        self.msg_input.insert("0.0", "{Cześć|Hej|Siema}! Sprawdź nasz serwer: discord.gg/xyz")

        # 3. DODATKOWE OPCJE (Friend Request)
        self.options_frame = ctk.CTkFrame(self.main_container)
        self.options_frame.pack(fill="x", pady=10)
        self.friend_req_var = ctk.BooleanVar(value=False)
        self.friend_req_check = ctk.CTkCheckBox(self.options_frame, text="Send Friend Request before DM", variable=self.friend_req_var)
        self.friend_req_check.pack(side="left", padx=20, pady=10)

        # 4. SEKCJE SCRAPERA / JOINERA / STATUSU (Uproszczony widok dla czytelności)
        # ... (Tu znajdują się sekcje z poprzednich kroków: status_frame, scrape_frame, joiner_frame) ...
        # [Dla oszczędności miejsca w odpowiedzi, zakładamy że są one podpięte tak samo]

        # 5. SEKCJA LOGÓW
        self.log_frame = ctk.CTkFrame(self.main_container)
        self.log_frame.pack(fill="both", expand=True, pady=10)
        self.log_box = ctk.CTkTextbox(self.log_frame, height=200, fg_color="#1a1a1a")
        self.log_box.pack(fill="both", padx=10, pady=10)

        # --- BOTTOM CONTROL BAR ---
        self.control_bar = ctk.CTkFrame(self, height=80)
        self.control_bar.grid(row=1, column=1, sticky="ew", padx=20, pady=(0, 20))
        self.start_btn = ctk.CTkButton(self.control_bar, text="START MISSION", fg_color="#2ecc71", command=self.start_mission)
        self.start_btn.pack(side="left", padx=50, pady=20)
        self.stop_btn = ctk.CTkButton(self.control_bar, text="STOP ALL", fg_color="#e74c3c", command=self.stop_all)
        self.stop_btn.pack(side="right", padx=50, pady=20)

    def add_log(self, message):
        self.log_box.insert("end", f"> {message}\n")
        self.log_box.see("end")

    def run_token_check(self):
        threading.Thread(target=self.token_mgr.check_all_accounts, daemon=True).start()

    def add_account(self):
        token = self.token_input.get()
        proxy = self.proxy_input.get()
        if token and self.db.add_account("discord", token, proxy):
            self.add_log(f"Added: {token[:15]}...")

    def start_mission(self):
        msg = self.msg_input.get("1.0", "end").strip()
        use_friend = self.friend_req_var.get()
        thread = threading.Thread(target=self.worker.run_mission, args=(msg, 10, 30, use_friend))
        thread.daemon = True
        thread.start()

    def stop_all(self):
        self.worker.stop()
        self.add_log("Stop requested.")

if __name__ == "__main__":
    app = MassDMApp()
    app.mainloop()
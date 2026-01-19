import customtkinter as ctk
import threading
from database import DatabaseManager
from discord_worker import DiscordWorker
from scraper import DiscordScraper
from status_changer import StatusChanger
from joiner import DiscordJoiner # Import nowego modułu

ctk.set_appearance_mode("Dark")
ctk.set_default_color_theme("blue")

class MassDMApp(ctk.CTk):
    def __init__(self):
        super().__init__()
        
        self.db = DatabaseManager()
        self.worker = DiscordWorker(self.db, self.add_log)
        self.scraper = DiscordScraper(self.db, self.add_log)
        self.status_changer = StatusChanger(self.db, self.add_log)
        self.joiner = DiscordJoiner(self.db, self.add_log) # Inicjalizacja

        self.title("Mass-DM Farm Tool Pro v1.0")
        self.geometry("1100x1000") # Zwiększona wysokość na nową sekcję

        self.grid_columnconfigure(1, weight=1)
        self.grid_rowconfigure(0, weight=1)

        # --- SIDEBAR ---
        self.sidebar = ctk.CTkFrame(self, width=200, corner_radius=0)
        self.sidebar.grid(row=0, column=0, sticky="nsew")
        self.logo = ctk.CTkLabel(self.sidebar, text="FARM TOOL", font=ctk.CTkFont(size=24, weight="bold"))
        self.logo.grid(row=0, column=0, padx=20, pady=30)
        self.btn_discord = ctk.CTkButton(self.sidebar, text="Discord Module")
        self.btn_discord.grid(row=1, column=0, padx=20, pady=10)

        # --- MAIN CONTENT AREA ---
        self.main_container = ctk.CTkScrollableFrame(self, fg_color="transparent")
        self.main_container.grid(row=0, column=1, padx=20, pady=20, sticky="nsew")

        # 1. SEKCA KONFIGURACJI (Konta + Proxy)
        self.acc_frame = ctk.CTkFrame(self.main_container)
        self.acc_frame.pack(fill="x", pady=10)
        ctk.CTkLabel(self.acc_frame, text="Account & Proxy Management", font=ctk.CTkFont(size=16, weight="bold")).grid(row=0, column=0, columnspan=2, pady=10)
        self.token_input = ctk.CTkEntry(self.acc_frame, placeholder_text="Discord Token", width=350)
        self.token_input.grid(row=1, column=0, padx=10, pady=5)
        self.proxy_input = ctk.CTkEntry(self.acc_frame, placeholder_text="Proxy (http://user:pass@ip:port)", width=350)
        self.proxy_input.grid(row=2, column=0, padx=10, pady=5)
        self.add_acc_btn = ctk.CTkButton(self.acc_frame, text="Add Account", command=self.add_account)
        self.add_acc_btn.grid(row=1, column=1, rowspan=2, padx=10, pady=5, sticky="ns")

        # 2. SEKCJA JOINERA (NOWOŚĆ)
        self.joiner_frame = ctk.CTkFrame(self.main_container)
        self.joiner_frame.pack(fill="x", pady=10)
        ctk.CTkLabel(self.joiner_frame, text="Server Joiner (Mass Join)", font=ctk.CTkFont(size=16, weight="bold")).grid(row=0, column=0, columnspan=2, pady=10)
        self.invite_input = ctk.CTkEntry(self.joiner_frame, placeholder_text="Invite Code or Link (e.g. discord.gg/xyz)", width=350)
        self.invite_input.grid(row=1, column=0, padx=10, pady=5)
        self.join_btn = ctk.CTkButton(self.joiner_frame, text="Join Server", fg_color="#f39c12", hover_color="#d35400", command=self.start_joining)
        self.join_btn.grid(row=1, column=1, padx=10, pady=5)

        # 3. SEKCJA STATUSU
        self.status_frame = ctk.CTkFrame(self.main_container)
        self.status_frame.pack(fill="x", pady=10)
        ctk.CTkLabel(self.status_frame, text="Status & Presence", font=ctk.CTkFont(size=16, weight="bold")).grid(row=0, column=0, columnspan=2, pady=10)
        self.status_text_input = ctk.CTkEntry(self.status_frame, placeholder_text="Custom Status", width=350)
        self.status_text_input.grid(row=1, column=0, padx=10, pady=5)
        self.status_text_input.insert(0, "Playing Metin2")
        self.status_type_var = ctk.StringVar(value="online")
        self.status_dropdown = ctk.CTkOptionMenu(self.status_frame, values=["online", "idle", "dnd", "invisible"], variable=self.status_type_var)
        self.status_dropdown.grid(row=1, column=1, padx=10, pady=5)
        self.update_status_btn = ctk.CTkButton(self.status_frame, text="Update Statuses", fg_color="#9b59b6", command=self.start_status_update)
        self.update_status_btn.grid(row=2, column=0, columnspan=2, pady=10)

        # 4. SEKCJA SCRAPERA
        self.scrape_frame = ctk.CTkFrame(self.main_container)
        self.scrape_frame.pack(fill="x", pady=10)
        ctk.CTkLabel(self.scrape_frame, text="Scraping Tools", font=ctk.CTkFont(size=16, weight="bold")).grid(row=0, column=0, columnspan=2, pady=10)
        self.scrape_channel_input = ctk.CTkEntry(self.scrape_frame, placeholder_text="Channel ID", width=350)
        self.scrape_channel_input.grid(row=1, column=0, padx=10, pady=5)
        self.scrape_btn = ctk.CTkButton(self.scrape_frame, text="Scrape Users", command=self.start_scraping)
        self.scrape_btn.grid(row=1, column=1, padx=10, pady=5)

        # 5. SEKCJA WIADOMOŚCI
        self.msg_frame = ctk.CTkFrame(self.main_container)
        self.msg_frame.pack(fill="x", pady=10)
        ctk.CTkLabel(self.msg_frame, text="Message Template", font=ctk.CTkFont(size=16, weight="bold")).pack(pady=5)
        self.msg_input = ctk.CTkTextbox(self.msg_frame, height=100)
        self.msg_input.pack(fill="x", padx=20, pady=10)

        # 6. SEKCJA LOGÓW
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

    def add_account(self):
        token = self.token_input.get()
        proxy = self.proxy_input.get()
        if token and self.db.add_account("discord", token, proxy):
            self.add_log(f"Account added: {token[:15]}...")
            self.token_input.delete(0, 'end')
            self.proxy_input.delete(0, 'end')

    def start_joining(self):
        invite = self.invite_input.get()
        if invite:
            thread = threading.Thread(target=self.joiner.run_mass_join, args=(invite,))
            thread.daemon = True
            thread.start()

    def start_scraping(self):
        token = self.token_input.get()
        channel_id = self.scrape_channel_input.get()
        if token and channel_id:
            thread = threading.Thread(target=self.scraper.scrape_history, args=(token, channel_id, 500))
            thread.daemon = True
            thread.start()

    def start_status_update(self):
        status_type = self.status_type_var.get()
        custom_text = self.status_text_input.get()
        thread = threading.Thread(target=self.status_changer.update_all_accounts, args=(status_type, custom_text))
        thread.daemon = True
        thread.start()

    def start_mission(self):
        msg = self.msg_input.get("1.0", "end").strip()
        thread = threading.Thread(target=self.worker.run_mission, args=(msg, 5, 10))
        thread.daemon = True
        thread.start()

    def stop_all(self):
        self.worker.stop()
        self.scraper.stop()
        self.status_changer.stop()
        self.joiner.stop()
        self.add_log("All processes stopped.")

if __name__ == "__main__":
    app = MassDMApp()
    app.mainloop()
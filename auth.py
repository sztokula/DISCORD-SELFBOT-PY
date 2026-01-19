import customtkinter as ctk
import threading
import time

# Ustawienia wyglądu
ctk.set_appearance_mode("Dark")
ctk.set_default_color_theme("blue")

class MassDMApp(ctk.CTk):
    def __init__(self):
        super().__init__()

        self.title("Mass-DM Farm Tool Pro v1.0")
        self.geometry("900x600")

        # Konfiguracja siatki (grid)
        self.grid_columnconfigure(1, weight=1)
        self.grid_rowconfigure(0, weight=1)

        # --- PASEK BOCZNY (Navigation) ---
        self.sidebar = ctk.CTkFrame(self, width=200, corner_radius=0)
        self.sidebar.grid(row=0, column=0, sticky="nsew")
        
        self.logo_label = ctk.CTkLabel(self.sidebar, text="FARM TOOL", font=ctk.CTkFont(size=20, weight="bold"))
        self.logo_label.grid(row=0, column=0, padx=20, pady=20)

        self.btn_discord = ctk.CTkButton(self.sidebar, text="Discord Module", command=self.show_discord)
        self.btn_discord.grid(row=1, column=0, padx=20, pady=10)

        self.btn_settings = ctk.CTkButton(self.sidebar, text="License & Settings", command=self.show_settings)
        self.btn_settings.grid(row=2, column=0, padx=20, pady=10)

        # --- PANEL GŁÓWNY (Dashboard) ---
        self.main_frame = ctk.CTkFrame(self, corner_radius=15, fg_color="transparent")
        self.main_frame.grid(row=0, column=1, padx=20, pady=20, sticky="nsew")

        self.label_status = ctk.CTkLabel(self.main_frame, text="System Ready", font=ctk.CTkFont(size=16))
        self.label_status.pack(pady=10)

        # Statystyki (Karty)
        self.stats_frame = ctk.CTkFrame(self.main_frame)
        self.stats_frame.pack(fill="x", padx=20, pady=10)
        
        self.stat_sent = ctk.CTkLabel(self.stats_frame, text="Sent: 0", width=150)
        self.stat_sent.pack(side="left", padx=20, pady=10)
        
        self.stat_errors = ctk.CTkLabel(self.stats_frame, text="Errors: 0", width=150, text_color="red")
        self.stat_errors.pack(side="left", padx=20, pady=10)

        # Konsola Logów
        self.log_box = ctk.CTkTextbox(self.main_frame, width=600, height=300)
        self.log_box.pack(padx=20, pady=20)
        self.add_log("Aplikacja uruchomiona pomyślnie...")

        # Przycisk START
        self.start_btn = ctk.CTkButton(self.main_frame, text="START MISSION", fg_color="green", hover_color="darkgreen", command=self.start_process)
        self.start_btn.pack(pady=10)

    def add_log(self, message):
        timestamp = time.strftime("%H:%M:%S")
        self.log_box.insert("end", f"[{timestamp}] {message}\n")
        self.log_box.see("end")

    def show_discord(self):
        self.add_log("Przełączono na moduł Discord.")

    def show_settings(self):
        self.add_log("Otwarto ustawienia licencji.")

    def start_process(self):
        # Uruchamiamy logikę w osobnym wątku, żeby nie zamrozić GUI
        self.add_log("Rozpoczynanie wysyłki...")
        thread = threading.Thread(target=self.fake_worker)
        thread.start()

    def fake_worker(self):
        # Symulacja pracy bota
        for i in range(1, 6):
            time.sleep(1)
            self.add_log(f"Wysyłanie wiadomości do użytkownika nr {i}...")
        self.add_log("Zakończono zadanie.")

if __name__ == "__main__":
    app = MassDMApp()
    app.mainloop()
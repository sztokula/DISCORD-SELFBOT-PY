import httpx
import time
import random

class DiscordJoiner:
    def __init__(self, db_manager, log_callback):
        self.db = db_manager
        self.log = log_callback
        self.is_running = False

    def join_server(self, token, invite_code, proxy=None):
        """invite_code: tylko końcówka linku, np. 'fajny-serwer' z discord.gg/fajny-serwer"""
        # Czyścimy kod zaproszenia na wypadek gdyby ktoś wkleił cały link
        invite_code = invite_code.split("/")[-1]
        
        url = f"https://discord.com/api/v9/invites/{invite_code}"
        headers = {
            "Authorization": token,
            "Content-Type": "application/json",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        }
        
        proxies = {"all://": proxy} if proxy else None
        
        try:
            with httpx.Client(proxies=proxies, headers=headers) as client:
                response = client.post(url, json={})
                if response.status_code == 200:
                    return True, "Sukces"
                elif response.status_code == 403:
                    return False, "Wymagana weryfikacja (Captcha/Telefon)"
                elif response.status_code == 429:
                    return False, "Rate Limit (za szybko!)"
                else:
                    return False, f"Błąd {response.status_code}"
        except Exception as e:
            return False, str(e)

    def run_mass_join(self, invite_code):
        self.is_running = True
        accounts = self.db.get_active_accounts("discord")
        
        if not accounts:
            self.log("[Joiner] Brak aktywnych kont do operacji.")
            return

        self.log(f"[Joiner] Rozpoczynam dołączanie {len(accounts)} kont do zaproszenia: {invite_code}")

        for acc in accounts:
            if not self.is_running: break
            
            acc_id, _, token, proxy, _, _, _, _ = acc
            success, msg = self.join_server(token, invite_code, proxy)
            
            if success:
                self.log(f"[Joiner] Konto {acc_id}: DOŁĄCZONO.")
            else:
                self.log(f"[Joiner] Konto {acc_id}: BŁĄD ({msg})")
            
            # BARDZO WAŻNE: Duży odstęp czasu przy dołączaniu
            wait = random.randint(10, 30)
            self.log(f"[Joiner] Oczekiwanie {wait}s przed następnym kontem...")
            time.sleep(wait)

        self.log("[Joiner] Proces masowego dołączania zakończony.")
        self.is_running = False

    def stop(self):
        self.is_running = False
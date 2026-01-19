import httpx
import time
import random

class StatusChanger:
    def __init__(self, db_manager, log_callback):
        self.db = db_manager
        self.log = log_callback
        self.is_running = False

    def change_status(self, token, status_type, custom_text, proxy=None):
        """
        status_type: 'online', 'idle', 'dnd', 'invisible'
        custom_text: np. 'Playing Metin2'
        """
        url = "https://discord.com/api/v9/users/@me/settings"
        headers = {
            "Authorization": token,
            "Content-Type": "application/json"
        }
        
        # Payload dla Discorda - ustawia status wizualny i tekstowy
        data = {
            "status": status_type,
            "custom_status": {"text": custom_text}
        }
        
        proxies = {"all://": proxy} if proxy else None
        
        try:
            with httpx.Client(proxies=proxies, headers=headers) as client:
                response = client.patch(url, json=data)
                if response.status_code == 200:
                    return True
                else:
                    self.log(f"[Status] Błąd {response.status_code} dla tokenu {token[:10]}...")
                    return False
        except Exception as e:
            self.log(f"[Status] Wyjątek: {str(e)}")
            return False

    def update_all_accounts(self, status_type, custom_text):
        self.is_running = True
        accounts = self.db.get_active_accounts("discord")
        
        if not accounts:
            self.log("[Status] Brak aktywnych kont do zmiany statusu.")
            return

        self.log(f"[Status] Zmieniam status dla {len(accounts)} kont na '{custom_text}'...")
        
        for acc in accounts:
            if not self.is_running: break
            
            acc_id, _, token, proxy, _, _, _, _ = acc
            success = self.change_status(token, status_type, custom_text, proxy)
            
            if success:
                self.log(f"[Status] Konto {acc_id} zaktualizowane.")
            
            # Mały delay, żeby nie wysłać wszystkiego w jednej sekundzie
            time.sleep(random.uniform(1.0, 3.0))
            
        self.log("[Status] Proces aktualizacji zakończony.")
        self.is_running = False

    def stop(self):
        self.is_running = False
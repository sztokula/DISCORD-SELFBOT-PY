import httpx
import time
import random

class StatusChanger:
    def __init__(self, db_manager, log_callback):
        self.db = db_manager
        self.log = log_callback
        self.is_running = False
        self.max_retries = 3
        self.backoff_factor = 1.5

    def _get_retry_after(self, response, default=5.0):
        retry_header = response.headers.get("Retry-After")
        if retry_header:
            try:
                return float(retry_header)
            except ValueError:
                pass
        try:
            data = response.json()
        except Exception:
            return default
        retry_after = data.get("retry_after")
        if retry_after is None:
            return default
        try:
            return float(retry_after)
        except (TypeError, ValueError):
            return default

    def _wait_for_rate_limit(self, response, attempt):
        retry_after = self._get_retry_after(response)
        wait_time = retry_after * (self.backoff_factor ** attempt)
        time.sleep(wait_time)

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
            with httpx.Client(proxies=proxies, headers=headers, timeout=httpx.Timeout(10.0)) as client:
                for attempt in range(self.max_retries + 1):
                    response = client.patch(url, json=data)
                    if response.status_code == 200:
                        return True
                    if response.status_code == 429:
                        self.log("[Status] Rate limit! Stosuję backoff...")
                        self._wait_for_rate_limit(response, attempt)
                        continue
                    self.log(f"[Status] Błąd {response.status_code} dla tokenu {token[:10]}...")
                    return False
                self.log(f"[Status] Rate limit przekroczony dla tokenu {token[:10]}...")
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

import httpx
import time
import random

class DiscordJoiner:
    def __init__(self, db_manager, log_callback):
        self.db = db_manager
        self.log = log_callback
        self.is_running = False

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
        
        max_retries = 3
        backoff_factor = 1.5

        try:
            with httpx.Client(proxies=proxies, headers=headers, timeout=httpx.Timeout(10.0)) as client:
                for attempt in range(max_retries + 1):
                    response = client.post(url, json={})
                    if response.status_code == 200:
                        return True, "Sukces"
                    elif response.status_code == 403:
                        return False, "Wymagana weryfikacja (Captcha/Telefon)"
                    elif response.status_code == 429:
                        retry_after = self._get_retry_after(response)
                        wait_time = retry_after * (backoff_factor ** attempt)
                        self._sleep_with_stop(wait_time)
                        continue
                    else:
                        return False, f"Błąd {response.status_code}"
        except Exception as e:
            return False, str(e)

        return False, "Rate Limit (po ponownych próbach)"

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
            self._sleep_with_stop(wait)

        self.log("[Joiner] Proces masowego dołączania zakończony.")
        self.is_running = False

    def stop(self):
        self.is_running = False

    def _sleep_with_stop(self, total_seconds, interval=0.5):
        end_time = time.monotonic() + max(0.0, total_seconds)
        while self.is_running and time.monotonic() < end_time:
            remaining = end_time - time.monotonic()
            time.sleep(min(interval, max(0.0, remaining)))

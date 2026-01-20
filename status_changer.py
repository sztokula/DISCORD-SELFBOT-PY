import httpx
import time
import random

class StatusChanger:
    def __init__(self, db_manager, log_callback):
        self.db = db_manager
        self.log = log_callback
        self.is_running = False
        self.auto_running = False
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

    def _wait_for_rate_limit(self, response, attempt, running_check=None):
        retry_after = self._get_retry_after(response)
        wait_time = retry_after * (self.backoff_factor ** attempt)
        self._sleep_with_stop(wait_time, running_check=running_check)

    def _sleep_with_stop(self, total_seconds, interval=0.5, running_check=None):
        if running_check is None:
            running_check = lambda: self.is_running
        end_time = time.monotonic() + max(0.0, total_seconds)
        while running_check() and time.monotonic() < end_time:
            remaining = end_time - time.monotonic()
            time.sleep(min(interval, max(0.0, remaining)))

    def _refresh_token(self, account_id, current_token):
        if account_id is None:
            return None
        try:
            fresh_token = self.db.get_account_token(account_id)
        except Exception as e:
            self.log(f"[Auth] Nie udaĹ‚o siÄ™ odĹ›wieĹĽyÄ‡ tokenu dla konta {account_id}: {e}")
            return None
        if fresh_token and fresh_token != current_token:
            self.log(f"[Auth] OdĹ›wieĹĽono token dla konta {account_id}.")
            return fresh_token
        return None

    def _handle_unauthorized(self, client, account_id, current_token, refreshed):
        if refreshed:
            self.log(f"[Status] Token dla konta {account_id} nadal niepoprawny. DezaktywujÄ™ konto.")
            if account_id is not None:
                self.db.update_account_status(account_id, "Banned/Dead")
                self.db.remove_account(account_id)
            return None, True
        new_token = self._refresh_token(account_id, current_token)
        if new_token:
            client.headers["Authorization"] = new_token
            return new_token, True
        self.log(f"[Status] Brak nowego tokenu dla konta {account_id}. DezaktywujÄ™ konto.")
        if account_id is not None:
            self.db.update_account_status(account_id, "Banned/Dead")
            self.db.remove_account(account_id)
        return None, True

    def change_status(self, account_id, token, status_type, custom_text, proxy=None):
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
                refreshed = False
                for attempt in range(self.max_retries + 1):
                    response = client.patch(url, json=data)
                    if response.status_code == 401:
                        token, refreshed = self._handle_unauthorized(client, account_id, token, refreshed)
                        if token:
                            continue
                        return False
                    if response.status_code in (200, 204):
                        if response.status_code == 204:
                            self.log("[Status] Status zaktualizowany (204 No Content).")
                        return True
                    if response.status_code == 429:
                        self.log("[Status] Rate limit! StosujÄ™ backoff...")
                        self._wait_for_rate_limit(response, attempt, running_check=lambda: self.is_running or self.auto_running)
                        continue
                    self.log(f"[Status] BĹ‚Ä…d {response.status_code} dla tokenu {token[:10]}...")
                    return False
                self.log(f"[Status] Rate limit przekroczony dla tokenu {token[:10]}...")
                return False
        except Exception as e:
            self.log(f"[Status] WyjÄ…tek: {str(e)}")
            return False

    def _update_all_accounts(self, status_type, custom_text, running_check):
        accounts = self.db.get_active_accounts("discord")
        
        if not accounts:
            self.log("[Status] Brak aktywnych kont do zmiany statusu.")
            return

        self.log(f"[Status] Zmieniam status dla {len(accounts)} kont na '{custom_text}'...")
        
        for acc in accounts:
            if not running_check():
                break
            
            acc_id, _, token, proxy, _, _, _, _, _, _, _ = acc
            success = self.change_status(acc_id, token, status_type, custom_text, proxy)
            
            if success:
                self.log(f"[Status] Konto {acc_id} zaktualizowane.")
            
            # MaĹ‚y delay, ĹĽeby nie wysĹ‚aÄ‡ wszystkiego w jednej sekundzie
            self._sleep_with_stop(random.uniform(1.0, 3.0), running_check=running_check)
            
        self.log("[Status] Proces aktualizacji zakoĹ„czony.")

    def update_all_accounts(self, status_type, custom_text):
        self.is_running = True
        try:
            self._update_all_accounts(status_type, custom_text, running_check=lambda: self.is_running)
        finally:
            self.is_running = False

    def run_auto_update(self, status_type, custom_text, delay_min_hours, delay_max_hours):
        if self.auto_running:
            self.log("[Status] Automatyczny status changer juĹĽ dziaĹ‚a.")
            return
        self.auto_running = True
        min_seconds = max(0.1, float(delay_min_hours)) * 3600.0
        max_seconds = max(min_seconds, float(delay_max_hours) * 3600.0)
        while self.auto_running:
            self.log("[Status] Start automatycznej aktualizacji statusĂłw.")
            self._update_all_accounts(status_type, custom_text, running_check=lambda: self.auto_running)
            if not self.auto_running:
                break
            wait_seconds = random.uniform(min_seconds, max_seconds)
            self.log(f"[Status] Kolejna aktualizacja za {wait_seconds / 3600:.2f}h.")
            self._sleep_with_stop(wait_seconds, running_check=lambda: self.auto_running)
        self.auto_running = False

    def stop(self):
        self.is_running = False
        self.auto_running = False



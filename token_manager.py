import time

import httpx

class TokenManager:
    def __init__(self, db_manager, log_callback):
        self.db = db_manager
        self.log = log_callback
        self.max_validation_retries = 2
        self.retry_backoff_seconds = 2.0

    def _fetch_token_info(self, token):
        url = "https://discord.com/api/v9/users/@me"
        headers = {"Authorization": token}
        try:
            with httpx.Client(headers=headers, timeout=httpx.Timeout(10.0)) as client:
                response = client.get(url)
        except Exception as exc:
            return "retry", f"Network error: {exc}"

        if response.status_code == 200:
            try:
                data = response.json()
            except Exception as exc:
                return "retry", f"Invalid response: {exc}"
            username = data.get("username")
            discriminator = data.get("discriminator")
            if not username or discriminator is None:
                return "retry", "Invalid response"
            return "ok", f"{username}#{discriminator}"
        if response.status_code == 401:
            return "unauthorized", "Invalid Token"
        return "retry", f"HTTP {response.status_code}"

    def validate_token(self, token):
        """Validate token and return username."""
        status, info = self._fetch_token_info(token)
        if status == "ok":
            return True, info
        if status == "unauthorized":
            return False, "Invalid Token"
        return False, f"Temporary error ({info}). Try again."

    def check_all_accounts(self):
        self.log("[Checker] Rozpoczynam weryfikację bazy tokenów...")
        accounts = self.db.get_active_accounts("discord")
        
        valid_count = 0
        for acc in accounts:
            acc_id, _, token, _, _, _, _, _, _, _, _ = acc

            status, info = self._fetch_token_info(token)
            attempts = 0
            while status == "retry" and attempts < self.max_validation_retries:
                attempts += 1
                time.sleep(self.retry_backoff_seconds * attempts)
                status, info = self._fetch_token_info(token)

            if status == "ok":
                self.log(f"[OK] Konto {acc_id}: {info}")
                valid_count += 1
                continue

            if status == "unauthorized":
                self.log(f"[DEAD] Konto {acc_id}: {info}. Dezaktywuję w bazie.")
                self.db.update_account_status(acc_id, "Banned/Dead")
                self.db.remove_account(acc_id)
                continue

            self.log(f"[WARN] Konto {acc_id}: {info}. Pomijam usuwanie (sprobuj ponownie pozniej).")
        
        self.log(f"[Checker] Zakończono. Aktywne: {valid_count}/{len(accounts)}")

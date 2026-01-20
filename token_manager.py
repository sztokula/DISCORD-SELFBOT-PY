import time

import httpx

class TokenManager:
    def __init__(self, db_manager, log_callback, metrics=None):
        self.db = db_manager
        self.log = log_callback
        self.metrics = metrics
        self.max_validation_retries = 2
        self.retry_backoff_seconds = 2.0

    def _fetch_token_info(self, token, proxy=None):
        url = "https://discord.com/api/v9/users/@me"
        headers = {"Authorization": token}
        proxies = {"all://": proxy} if proxy else None
        try:
            with httpx.Client(proxies=proxies, headers=headers, timeout=httpx.Timeout(10.0)) as client:
                start = time.monotonic()
                response = client.get(url)
        except Exception as exc:
            return "retry", f"Network error: {exc}"
        finally:
            if self.metrics:
                duration = time.monotonic() - start if "start" in locals() else 0.0
                self.metrics.record_request(duration, rate_limited=False)

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

    def validate_token(self, token, proxy=None):
        """Validate token and return status + info."""
        status, info = self._fetch_token_info(token, proxy)
        if status == "unauthorized":
            return "unauthorized", "Invalid Token"
        if status == "ok":
            return "ok", info
        return "retry", info

    def check_all_accounts(self):
        self.log("[Checker] Rozpoczynam weryfikację bazy tokenów...")
        accounts = self.db.get_active_accounts("discord")
        
        valid_count = 0
        for acc in accounts:
            acc_id, _, token, proxy, _, _, _, _, _, _, _ = acc

            status, info = self._fetch_token_info(token, proxy)
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

import time

import httpx

class TokenManager:
    def __init__(self, db_manager, log_callback, metrics=None):
        self.db = db_manager
        self.log = log_callback
        self.metrics = metrics
        self.max_validation_retries = 2
        self.retry_backoff_seconds = 2.0
        self._proxy_check_cache = {}
        self._proxy_check_ttl_seconds = 600

    def _is_proxy_required(self):
        try:
            value = self.db.get_setting("require_proxy", None)
        except Exception:
            return False
        if value in (None, ""):
            return False
        if isinstance(value, str):
            return value.strip().lower() in {"1", "true", "yes", "on"}
        return bool(value)

    def _check_proxy_alive(self, proxy):
        if not proxy:
            return False, "Proxy is empty."
        cached = self._proxy_check_cache.get(proxy)
        now = time.monotonic()
        if cached and (now - cached["ts"]) < self._proxy_check_ttl_seconds:
            return cached["ok"], cached["err"]
        try:
            with httpx.Client(
                proxies={"all://": proxy},
                timeout=httpx.Timeout(8.0),
                headers={"User-Agent": "Mozilla/5.0"},
            ) as client:
                resp = client.get("https://discord.com/api/v9/experiments")
            if resp.status_code == 407:
                ok, err = False, "Proxy auth failed (407)."
            else:
                ok, err = True, None
        except Exception as exc:
            ok, err = False, f"Proxy error: {exc}"
        self._proxy_check_cache[proxy] = {"ok": ok, "err": err, "ts": now}
        return ok, err

    def _fetch_token_info(self, token, proxy=None):
        if self._is_proxy_required() and not proxy:
            return "retry", "Proxy is required."
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
                rate_limited = False
                status_code = None
                if "response" in locals():
                    status_code = response.status_code
                    rate_limited = status_code == 429
                self.metrics.record_request(duration, status_code=status_code, rate_limited=rate_limited)

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
        self.log("[Checker] Starting token database verification...")
        accounts = self.db.get_active_accounts("discord")
        
        valid_count = 0
        for acc in accounts:
            acc_id, _, token, proxy, _, _, _, _, _, _, _ = acc
            if self._is_proxy_required():
                if not proxy:
                    self.log(f"[Checker] Account {acc_id}: proxy required (skipping).")
                    continue
                ok, err = self._check_proxy_alive(proxy)
                if not ok:
                    self.log(f"[Checker] Account {acc_id}: {err} (skipping).")
                    continue

            status, info = self._fetch_token_info(token, proxy)
            attempts = 0
            while status == "retry" and attempts < self.max_validation_retries:
                attempts += 1
                time.sleep(self.retry_backoff_seconds * attempts)
                status, info = self._fetch_token_info(token, proxy)

            if status == "ok":
                self.log(f"[OK] Account {acc_id}: {info}")
                valid_count += 1
                continue

            if status == "unauthorized":
                self.log(f"[DEAD] Account {acc_id}: {info}. Deactivating in database.")
                self.db.update_account_status(acc_id, "Banned/Dead")
                self.db.remove_account(acc_id)
                continue

            self.log(f"[WARN] Account {acc_id}: {info}. Skipping removal (retry later).")
        
        self.log(f"[Checker] Done. Active: {valid_count}/{len(accounts)}")

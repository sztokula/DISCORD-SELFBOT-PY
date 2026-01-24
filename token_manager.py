import time
from urllib.parse import urlparse

import httpx

from proxy_utils import normalize_proxy, httpx_client
from super_properties import set_super_properties_header
from client_identity import USER_AGENT

DEFAULT_PROXY_CHECK_ENDPOINT = "https://discord.com/api/v9/experiments"

class TokenManager:
    def __init__(self, db_manager, log_callback, metrics=None, telemetry=None):
        self.db = db_manager
        self.log = log_callback
        self.metrics = metrics
        self.telemetry = telemetry
        self.max_validation_retries = 2
        self.retry_backoff_seconds = 2.0
        self._proxy_check_cache = {}
        self._proxy_check_ttl_seconds = 600

    def _is_proxy_required(self):
        try:
            value = self.db.get_setting("require_proxy", None)
        except Exception as exc:
            if self.log:
                self.log(f"[Token] Failed to read require_proxy setting: {type(exc).__name__}")
            return False
        if value in (None, ""):
            return False
        if isinstance(value, str):
            return value.strip().lower() in {"1", "true", "yes", "on"}
        return bool(value)

    def _check_proxy_alive(self, proxy):
        if not proxy:
            return False, "Proxy is empty."
        proxy = normalize_proxy(proxy)
        if not proxy:
            return False, "Invalid proxy format."
        cached = self._proxy_check_cache.get(proxy)
        now = time.monotonic()
        if cached and (now - cached["ts"]) < self._proxy_check_ttl_seconds:
            return cached["ok"], cached["err"]
        try:
            user_agent = USER_AGENT
            with httpx_client(
                proxy,
                timeout=httpx.Timeout(8.0),
                headers={"User-Agent": user_agent},
            ) as client:
                set_super_properties_header(client.headers, self.db)
                resp = client.get(self._get_proxy_check_endpoint())
            if resp.status_code == 407:
                ok, err = False, "Proxy auth failed (407)."
            else:
                ok, err = True, None
        except Exception as exc:
            ok, err = False, f"Proxy error: {exc}"
        self._proxy_check_cache[proxy] = {"ok": ok, "err": err, "ts": now}
        return ok, err

    def _get_proxy_check_endpoint(self):
        raw = (self.db.get_setting("proxy_check_endpoint", "") or "").strip()
        if not raw:
            return DEFAULT_PROXY_CHECK_ENDPOINT
        parsed = urlparse(raw)
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            return DEFAULT_PROXY_CHECK_ENDPOINT
        return raw

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

    def _fetch_token_info(self, token, proxy=None):
        if self._is_proxy_required() and not proxy:
            return "retry", "Proxy is required."
        if proxy:
            proxy = normalize_proxy(proxy)
            if not proxy:
                return "retry", "Invalid proxy format."
        url = "https://discord.com/api/v9/users/@me"
        user_agent = USER_AGENT
        headers = {"Authorization": token, "User-Agent": user_agent}
        set_super_properties_header(headers, self.db)
        try:
            with httpx_client(
                proxy,
                headers=headers,
                timeout=httpx.Timeout(10.0),
                cookie_db=self.db,
                cookie_token=token,
            ) as client:
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
                try:
                    self.db.record_token_violation(
                        token,
                        "silent_fail",
                        severity=1,
                        status="suspected",
                        cooldown_seconds=60,
                        details="missing_username_discriminator",
                    )
                except Exception as exc:
                    if self.log:
                        self.log(f"[Token] Failed to record silent_fail: {type(exc).__name__}")
                return "retry", "Invalid response"
            if self.telemetry:
                self.telemetry.send_science(
                    token,
                    user_agent,
                    "token_check",
                    properties={"username": username, "discriminator": discriminator},
                    proxy=proxy,
                )
            return "ok", f"{username}#{discriminator}"
        if response.status_code in (401, 403):
            try:
                self.db.clear_token_cookies(token)
            except Exception as exc:
                if self.log:
                    self.log(f"[Cookies] Failed to clear cookies after auth error: {type(exc).__name__}")
            if response.status_code == 403:
                try:
                    self.db.record_token_violation(
                        token,
                        "forbidden",
                        severity=2,
                        status="limited",
                        cooldown_seconds=300,
                    )
                except Exception as exc:
                    if self.log:
                        self.log(f"[Token] Failed to record forbidden: {type(exc).__name__}")
                return "retry", "Forbidden"
            return "unauthorized", "Invalid Token"
        if response.status_code == 429:
            retry_after = self._get_retry_after(response, default=30.0)
            try:
                self.db.record_token_violation(
                    token,
                    "rate_limited",
                    severity=2,
                    status="cooldowned",
                    cooldown_seconds=retry_after,
                )
            except Exception as exc:
                if self.log:
                    self.log(f"[Token] Failed to record rate_limited: {type(exc).__name__}")
            return "retry", f"HTTP {response.status_code}"
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
                self.log(f"[Info] Account {acc_id}: {info}")
                valid_count += 1
                continue

            if status == "unauthorized":
                self.log(f"[Error] Account {acc_id}: {info}. Deactivating in database.")
                self.db.update_account_status(acc_id, "Banned/Dead")
                self.db.remove_account(acc_id)
                continue

            self.log(f"[Warn] Account {acc_id}: {info}. Skipping removal (retry later).")
        
        self.log(f"[Checker] Done. Active: {valid_count}/{len(accounts)}")

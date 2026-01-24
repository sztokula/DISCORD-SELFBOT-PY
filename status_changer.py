import httpx
import time
from datetime import datetime
from proxy_utils import httpx_client
from super_properties import set_super_properties_header
from client_identity import USER_AGENT
from behavior_version import CURRENT_BEHAVIOR_VERSION, get_behavior_version, seeded_rng

class StatusChanger:
    def __init__(self, db_manager, log_callback, metrics=None, telemetry=None):
        self.db = db_manager
        self.log = log_callback
        self.metrics = metrics
        self.telemetry = telemetry
        self.is_running = False
        self.auto_running = False
        self.max_retries = 3
        self.backoff_factor = 1.5

    def _record_request(self, duration, response=None):
        if not self.metrics:
            return
        status_code = response.status_code if response is not None else None
        rate_limited = status_code == 429
        self.metrics.record_request(duration, status_code=status_code, rate_limited=rate_limited)

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

    def _get_setting_float(self, key, default, min_value=None, max_value=None):
        try:
            raw = self.db.get_setting(key, "")
        except Exception as exc:
            if self.log:
                self.log(f"[Status] Failed to read setting {key}: {type(exc).__name__}")
            raw = ""
        if raw in (None, ""):
            value = default
        else:
            try:
                value = float(raw)
            except (TypeError, ValueError):
                value = default
        if min_value is not None:
            value = max(min_value, value)
        if max_value is not None:
            value = min(max_value, value)
        return value

    def _recent_profile_change(self, account_id):
        min_gap = self._get_setting_float("profile_status_min_gap_seconds", 900.0, 0.0, None)
        if not min_gap:
            return False, None
        try:
            history = self.db.get_profile_history(account_id) or {}
        except Exception as exc:
            if self.log:
                self.log(f"[Status] Failed to read profile history: {type(exc).__name__}")
            return False, None
        updated_at = history.get("updated_at")
        if not updated_at:
            return False, None
        try:
            updated_dt = datetime.fromisoformat(str(updated_at))
        except Exception as exc:
            if self.log:
                self.log(f"[Status] Failed to parse profile timestamp: {type(exc).__name__}")
            return False, None
        elapsed = (datetime.now() - updated_dt).total_seconds()
        if elapsed < min_gap:
            return True, min_gap - elapsed
        return False, None

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
            self.log(f"[Auth] Failed to refresh token for account {account_id}: {e}")
            return None
        if fresh_token and fresh_token != current_token:
            self.log(f"[Auth] Refreshed token for account {account_id}.")
            return fresh_token
        return None

    def _handle_unauthorized(self, client, account_id, current_token, refreshed):
        if current_token:
            try:
                self.db.clear_token_cookies(current_token)
            except Exception as exc:
                if self.log:
                    self.log(
                        f"[Cookies] Account {account_id}: failed to clear cookies ({type(exc).__name__})."
                    )
        if refreshed:
            self.log(f"[Status] Token for account {account_id} is still invalid. Deactivating account.")
            if account_id is not None:
                self.db.update_account_status(account_id, "Banned/Dead")
                self.db.remove_account(account_id)
            return None, True
        new_token = self._refresh_token(account_id, current_token)
        if new_token:
            client.headers["Authorization"] = new_token
            return new_token, True
        self.log(f"[Status] No new token for account {account_id}. Deactivating account.")
        if account_id is not None:
            self.db.update_account_status(account_id, "Banned/Dead")
            self.db.remove_account(account_id)
        return None, True

    def change_status(self, account_id, token, status_type, custom_text, proxy=None):
        """
        status_type: 'online', 'idle', 'dnd', 'invisible'
        custom_text: e.g. 'Playing Metin2'
        """
        recent, remaining = self._recent_profile_change(account_id)
        if recent:
            self.log(f"[Status] Skipped account {account_id}: recent profile change ({int(remaining)}s remaining).")
            return False

        url = "https://discord.com/api/v9/users/@me/settings"
        user_agent = USER_AGENT
        headers = {
            "Authorization": token,
            "Content-Type": "application/json"
        }
        headers["User-Agent"] = user_agent
        set_super_properties_header(headers, self.db)
        
        # Discord payload - sets visual and custom status.
        data = {
            "status": status_type,
            "custom_status": {"text": custom_text}
        }
        
        try:
            with httpx_client(
                proxy,
                headers=headers,
                timeout=httpx.Timeout(10.0),
                cookie_db=self.db,
                cookie_token=lambda: token,
            ) as client:
                refreshed = False
                for attempt in range(self.max_retries + 1):
                    start = time.monotonic()
                    response = client.patch(url, json=data)
                    self._record_request(time.monotonic() - start, response)
                    if response.status_code == 401:
                        token, refreshed = self._handle_unauthorized(client, account_id, token, refreshed)
                        if token:
                            continue
                        return False
                    if response.status_code in (200, 204):
                        if response.status_code == 204:
                            self.log("[Status] Status updated (204 No Content).")
                        self.log(f"[Info] Status updated for account {account_id}.")
                        now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        try:
                            self.db.update_profile_history(
                                account_id,
                                status_text=custom_text,
                                status_updated_at=now_str,
                                updated_at=now_str,
                            )
                        except Exception as exc:
                            if self.log:
                                self.log(
                                    f"[Status] Account {account_id}: failed to save profile history "
                                    f"({type(exc).__name__})."
                                )
                        if self.telemetry:
                            self.telemetry.send_science(
                                token,
                                user_agent,
                                "status_update",
                                properties={"status": status_type, "custom_text": custom_text},
                                proxy=proxy,
                            )
                        return True
                    if response.status_code == 403:
                        try:
                            self.db.clear_token_cookies(token)
                        except Exception as exc:
                            if self.log:
                                self.log(
                                    f"[Cookies] Account {account_id}: failed to clear cookies ({type(exc).__name__})."
                                )
                        self.log(f"[Status] Forbidden (403) for token {token[:10]}...")
                        return False
                    if response.status_code == 429:
                        self.log("[Status] Rate limit. Applying backoff...")
                        self._wait_for_rate_limit(response, attempt, running_check=lambda: self.is_running or self.auto_running)
                        continue
                    self.log(f"[Status] Error {response.status_code} for token {token[:10]}...")
                    return False
                self.log(f"[Status] Rate limit exceeded for token {token[:10]}...")
                return False
        except Exception as e:
            self.log(f"[Status] Exception: {str(e)}")
            return False

    def _update_all_accounts(self, status_type, custom_text, running_check, allowed_account_ids=None):
        accounts = self.db.get_active_accounts("discord")
        if allowed_account_ids:
            allowed_set = set(allowed_account_ids)
            accounts = [acc for acc in accounts if acc[0] in allowed_set]
        
        if not accounts:
            self.log("[Status] No active accounts to update status.")
            return

        self.log(f"[Status] Updating status for {len(accounts)} accounts to '{custom_text}'...")
        
        for acc in accounts:
            if not running_check():
                break
            
            acc_id, _, token, proxy, _, _, _, _, _, _, _ = acc
            success = self.change_status(acc_id, token, status_type, custom_text, proxy)
            
            if success:
                self.log(f"[Status] Account {acc_id} updated.")
            
            # Small delay to avoid sending everything in a single second.
            version = get_behavior_version(self.db, token, CURRENT_BEHAVIOR_VERSION)
            base_rng = seeded_rng(token or "anon", version, "status_delay")
            scale = base_rng.uniform(0.8, 1.2)
            delay = base_rng.uniform(1.0 * scale, 3.0 * scale)
            self._sleep_with_stop(delay, running_check=running_check)
            
        self.log("[Status] Status update finished.")

    def update_all_accounts(self, status_type, custom_text, allowed_account_ids=None):
        self.is_running = True
        try:
            self._update_all_accounts(
                status_type,
                custom_text,
                running_check=lambda: self.is_running,
                allowed_account_ids=allowed_account_ids,
            )
        finally:
            self.is_running = False

    def run_auto_update(
        self,
        status_type,
        custom_text,
        delay_min_hours,
        delay_max_hours,
        allowed_account_ids=None,
    ):
        if self.auto_running:
            self.log("[Status] Auto status updater is already running.")
            return
        self.auto_running = True
        min_seconds = max(0.1, float(delay_min_hours)) * 3600.0
        max_seconds = max(min_seconds, float(delay_max_hours) * 3600.0)
        base_rng = seeded_rng("auto_status", CURRENT_BEHAVIOR_VERSION, "auto_status")
        while self.auto_running:
            self.log("[Status] Starting automatic status update.")
            self._update_all_accounts(
                status_type,
                custom_text,
                running_check=lambda: self.auto_running,
                allowed_account_ids=allowed_account_ids,
            )
            if not self.auto_running:
                break
            wait_seconds = base_rng.uniform(min_seconds, max_seconds)
            self.log(f"[Status] Next update in {wait_seconds / 3600:.2f}h.")
            self._sleep_with_stop(wait_seconds, running_check=lambda: self.auto_running)
        self.auto_running = False

    def stop(self):
        self.is_running = False
        self.auto_running = False



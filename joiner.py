import httpx
import time
import random

class DiscordJoiner:
    def __init__(self, db_manager, log_callback, captcha_solver=None, metrics=None):
        self.db = db_manager
        self.log = log_callback
        self.is_running = False
        self.captcha_solver = captcha_solver
        self.metrics = metrics

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
        if refreshed:
            self.log(f"[Joiner] Token for account {account_id} is still invalid. Deactivating account.")
            if account_id is not None:
                self.db.update_account_status(account_id, "Banned/Dead")
                self.db.remove_account(account_id)
            return None, True
        new_token = self._refresh_token(account_id, current_token)
        if new_token:
            client.headers["Authorization"] = new_token
            return new_token, True
        self.log(f"[Joiner] No new token for account {account_id}. Deactivating account.")
        if account_id is not None:
            self.db.update_account_status(account_id, "Banned/Dead")
            self.db.remove_account(account_id)
        return None, True

    def join_server(self, account_id, token, invite_code, proxy=None):
        """invite_code: only the invite slug, e.g. 'cool-server' from discord.gg/cool-server"""
        # Strip invite code in case a full link is pasted.
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
                refreshed = False
                for attempt in range(max_retries + 1):
                    start = time.monotonic()
                    response = client.post(url, json={})
                    self._record_request(time.monotonic() - start, response)
                    if response.status_code == 401:
                        token, refreshed = self._handle_unauthorized(client, account_id, token, refreshed)
                        if token:
                            continue
                        return False, "Unauthorized (token)"
                    if response.status_code == 200:
                        return True, "Success"
                    if response.status_code in {400, 403}:
                        captcha_info = self._extract_captcha(response)
                        if captcha_info and self.captcha_solver:
                            solved, token_or_err = self.captcha_solver.solve_captcha(captcha_info)
                            if solved:
                                payload = {"captcha_key": token_or_err}
                                if captcha_info.get("rqtoken"):
                                    payload["captcha_rqtoken"] = captcha_info["rqtoken"]
                                start = time.monotonic()
                                retry_resp = client.post(url, json=payload)
                                self._record_request(time.monotonic() - start, retry_resp)
                                if retry_resp.status_code == 200:
                                    return True, "Success (captcha)"
                                return False, f"Post-captcha error: {retry_resp.status_code}"
                            return False, f"Captcha error: {token_or_err}"
                        return False, "Verification required (Captcha/Phone)"
                    if response.status_code == 429:
                        retry_after = self._get_retry_after(response)
                        wait_time = retry_after * (backoff_factor ** attempt)
                        self._sleep_with_stop(wait_time)
                        continue
                    return False, f"Error {response.status_code}"
        except Exception as e:
            return False, str(e)

        return False, "Rate Limit (after retries)"

    def run_mass_join(self, invite_codes, delay_min, delay_max, on_complete=None):
        self.is_running = True
        self.db.reset_daily_counters()
        accounts = self.db.get_active_accounts("discord")
        joined_any = False
        
        if not accounts:
            self.log("[Joiner] No active accounts available.")
            if on_complete:
                on_complete(False)
            return

        if not invite_codes:
            self.log("[Joiner] No valid invites.")
            self.is_running = False
            if on_complete:
                on_complete(False)
            return

        self.log(f"[Joiner] Starting to join {len(accounts)} accounts to {len(invite_codes)} invites (random per account).")

        did_join_attempt = False
        for acc in accounts:
            if not self.is_running: break
            
            acc_id, _, token, proxy, _, _, _, _, join_limit, join_today, _ = acc
            if join_today >= join_limit:
                self.log(f"[Joiner] Account {acc_id}: daily join limit reached ({join_today}/{join_limit}).")
                continue
            invite_code = random.choice(invite_codes)
            success, msg = self.join_server(acc_id, token, invite_code, proxy)
            
            if success:
                self.db.increment_join_counter(acc_id)
                joined_any = True
                self.log(f"[Joiner] Account {acc_id}: JOINED ({invite_code}).")
            else:
                self.log(f"[Joiner] Account {acc_id}: ERROR ({msg}) [{invite_code}]")
            
            did_join_attempt = True
            # IMPORTANT: add a larger delay between joins.
            wait = random.randint(delay_min, delay_max)
            self.log(f"[Joiner] Waiting {wait}s before the next account...")
            self._sleep_with_stop(wait)

        if self.is_running and not did_join_attempt:
            self.log("[Joiner] All accounts reached the daily join limit.")
        self.log("[Joiner] Mass join process finished.")
        self.is_running = False
        if on_complete:
            on_complete(joined_any)

    def stop(self):
        self.is_running = False

    def _sleep_with_stop(self, total_seconds, interval=0.5):
        end_time = time.monotonic() + max(0.0, total_seconds)
        while self.is_running and time.monotonic() < end_time:
            remaining = end_time - time.monotonic()
            time.sleep(min(interval, max(0.0, remaining)))

    def _extract_captcha(self, response):
        try:
            data = response.json()
        except Exception:
            return None
        sitekey = data.get("captcha_sitekey")
        if not sitekey:
            return None
        service = data.get("captcha_service") or "hcaptcha"
        return {
            "service": service,
            "sitekey": sitekey,
            "rqdata": data.get("captcha_rqdata"),
            "rqtoken": data.get("captcha_rqtoken"),
            "url": "https://discord.com",
        }

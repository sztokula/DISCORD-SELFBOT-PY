import httpx
import time
import random
import re

class DiscordWorker:
    def __init__(self, db_manager, log_callback, metrics=None, captcha_solver=None):
        self.db = db_manager
        self.log = log_callback
        self.metrics = metrics
        self.captcha_solver = captcha_solver
        self.is_running = False
        self.max_retries = 3
        self.backoff_factor = 1.5
        self.default_tags = ["#promo", "#info", "#discord", "#community", "#support"]
        self.default_emojis = ["🔥", "✨", "✅", "🚀", "🎉", "💬", "🧩", "🌟"]
        self._last_template = None

    def _record_request(self, duration, response=None):
        if not self.metrics:
            return
        status_code = response.status_code if response is not None else None
        rate_limited = status_code == 429
        self.metrics.record_request(duration, status_code=status_code, rate_limited=rate_limited)

    def parse_spintax(self, text):
        """Replace {option1|option2} with a random option."""
        while True:
            match = re.search(r"\{([^{}]*)\}", text)
            if not match:
                break
            options = [option for option in match.group(1).split("|") if option]
            text = text.replace(match.group(0), random.choice(options) if options else "", 1)
        return text

    def _choose_custom_list(self, raw_value, fallback):
        if not raw_value:
            return fallback
        items = [item.strip() for item in raw_value.split(",") if item.strip()]
        return items if items else fallback

    def _replace_random_tokens(self, text):
        def replace_tag(match):
            custom = match.group(1)
            options = self._choose_custom_list(custom, self.default_tags)
            return random.choice(options)

        def replace_emoji(match):
            custom = match.group(1)
            options = self._choose_custom_list(custom, self.default_emojis)
            return random.choice(options)

        def replace_num(match):
            start = match.group(1)
            end = match.group(2)
            if start and end:
                start_val = int(start)
                end_val = int(end)
            else:
                start_val = 1
                end_val = 999
            if start_val > end_val:
                start_val, end_val = end_val, start_val
            return str(random.randint(start_val, end_val))

        text = re.sub(r"\[\[tag(?::([^\]]+))?\]\]", replace_tag, text)
        text = re.sub(r"\[\[emoji(?::([^\]]+))?\]\]", replace_emoji, text)
        text = re.sub(r"\[\[num(?::(\d+)-(\d+))?\]\]", replace_num, text)
        return text

    def render_message(self, template):
        message = self._replace_random_tokens(template)
        return self.parse_spintax(message)

    def _pick_template(self, templates):
        if not templates:
            return ""
        if len(templates) == 1:
            chosen = templates[0]
            self._last_template = chosen
            return chosen
        candidates = [tpl for tpl in templates if tpl != self._last_template]
        if not candidates:
            candidates = templates
        chosen = random.choice(candidates)
        self._last_template = chosen
        return chosen

    def send_friend_request(self, client, user_id):
        """Optional helper that sends a friend request."""
        url = f"https://discord.com/api/v9/users/{user_id}/relationships"
        for attempt in range(self.max_retries + 1):
            try:
                start = time.monotonic()
                resp = client.put(url, json={})
                self._record_request(time.monotonic() - start, resp)
            except Exception as e:
                self.log(f"[Friend Request] Exception for user {user_id}: {e}")
                return False
            if resp.status_code in {200, 204}:
                return True
            if resp.status_code == 429:
                self._wait_for_rate_limit(resp, attempt)
                continue
            if resp.status_code in {400, 403}:
                captcha_payload, err = self._solve_captcha_payload(resp)
                if not captcha_payload:
                    self.log(f"[Captcha] Friend request blocked for {user_id}: {err}")
                    return False
                start = time.monotonic()
                retry_resp = client.put(url, json=captcha_payload)
                self._record_request(time.monotonic() - start, retry_resp)
                if retry_resp.status_code in {200, 204}:
                    return True
                if retry_resp.status_code == 429:
                    self._wait_for_rate_limit(retry_resp, attempt)
                    continue
                self.log(
                    f"[Friend Request] Post-captcha error for {user_id}: {retry_resp.status_code}"
                )
                return False
            self.log(f"[Friend Request] Error for {user_id}: {resp.status_code}")
            return False
        return False

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
        self._sleep_with_stop(wait_time)

    def _sleep_with_stop(self, total_seconds, interval=0.5):
        end_time = time.monotonic() + max(0.0, total_seconds)
        while self.is_running and time.monotonic() < end_time:
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
        if refreshed:
            self.log(f"[Auth] Token for account {account_id} is still invalid. Deactivating account.")
            if account_id is not None:
                self.db.update_account_status(account_id, "Banned/Dead")
                self.db.remove_account(account_id)
            return None, True
        new_token = self._refresh_token(account_id, current_token)
        if new_token:
            client.headers["Authorization"] = new_token
            return new_token, True
        self.log(f"[Auth] No new token for account {account_id}. Deactivating account.")
        if account_id is not None:
            self.db.update_account_status(account_id, "Banned/Dead")
            self.db.remove_account(account_id)
        return None, True

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

    def _solve_captcha_payload(self, response):
        captcha_info = self._extract_captcha(response)
        if not captcha_info:
            return None, "Captcha required."
        if not self.captcha_solver:
            return None, "Captcha solver not configured."
        solved, token_or_err = self.captcha_solver.solve_captcha(captcha_info)
        if not solved:
            return None, f"Captcha error: {token_or_err}"
        payload = {"captcha_key": token_or_err}
        if captcha_info.get("rqtoken"):
            payload["captcha_rqtoken"] = captcha_info["rqtoken"]
        return payload, None

    def send_dm(
        self,
        account_id,
        token,
        user_id,
        message_template,
        proxy=None,
        add_friend=False,
        friend_delay_min=0,
        friend_delay_max=0,
        dry_run=False,
    ):
        if dry_run:
            final_msg = self.render_message(message_template)
            if add_friend:
                self.log(f"[Dry-Run] Would send friend request to {user_id}.")
                if friend_delay_max > 0:
                    delay = random.randint(friend_delay_min, friend_delay_max)
                    if delay > 0:
                        self.log(f"[Dry-Run] Waiting {delay}s before DM to {user_id}.")
                        self._sleep_with_stop(delay)
            return True, final_msg
        headers = {
            "Authorization": token,
            "Content-Type": "application/json",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        }
        proxies = {"all://": proxy} if proxy else None
        
        with httpx.Client(proxies=proxies, headers=headers, timeout=httpx.Timeout(10.0)) as client:
            try:
                refreshed = False
                # Optional: send friend request.
                if add_friend:
                    self.send_friend_request(client, user_id)
                    if friend_delay_max > 0:
                        delay = random.randint(friend_delay_min, friend_delay_max)
                        if delay > 0:
                            self.log(f"[Friend Request] Waiting {delay}s before DM to {user_id}.")
                            self._sleep_with_stop(delay)

                # Open DM channel.
                url_channel = "https://discord.com/api/v9/users/@me/channels"
                channel_id = None
                for attempt in range(self.max_retries + 1):
                    start = time.monotonic()
                    response = client.post(url_channel, json={"recipient_id": user_id})
                    self._record_request(time.monotonic() - start, response)
                    if response.status_code == 401:
                        token, refreshed = self._handle_unauthorized(client, account_id, token, refreshed)
                        if token:
                            continue
                        return False, "Unauthorized (token)"
                    if response.status_code == 200:
                        channel_id = response.json().get("id")
                        break
                    if response.status_code == 429:
                        self._wait_for_rate_limit(response, attempt)
                        continue
                    if response.status_code in {400, 403}:
                        captcha_payload, err = self._solve_captcha_payload(response)
                        if not captcha_payload:
                            return False, err
                        captcha_payload["recipient_id"] = user_id
                        start = time.monotonic()
                        retry_resp = client.post(url_channel, json=captcha_payload)
                        self._record_request(time.monotonic() - start, retry_resp)
                        if retry_resp.status_code == 401:
                            token, refreshed = self._handle_unauthorized(client, account_id, token, refreshed)
                            if token:
                                continue
                            return False, "Unauthorized (token)"
                        if retry_resp.status_code == 200:
                            channel_id = retry_resp.json().get("id")
                            break
                        if retry_resp.status_code == 429:
                            self._wait_for_rate_limit(retry_resp, attempt)
                            continue
                        return False, f"Channel captcha error: {retry_resp.status_code}"
                    return False, f"Channel Error: {response.status_code}"

                if not channel_id:
                    return False, "Rate Limit (DM channel)"

                msg_url = f"https://discord.com/api/v9/channels/{channel_id}/messages"

                # Message randomization (templates + spintax).
                final_msg = self.render_message(message_template)

                for attempt in range(self.max_retries + 1):
                    start = time.monotonic()
                    msg_resp = client.post(msg_url, json={"content": final_msg})
                    self._record_request(time.monotonic() - start, msg_resp)
                    if msg_resp.status_code == 401:
                        token, refreshed = self._handle_unauthorized(client, account_id, token, refreshed)
                        if token:
                            continue
                        return False, "Unauthorized (token)"
                    if msg_resp.status_code == 200:
                        return True, "Success"
                    if msg_resp.status_code == 429:
                        self._wait_for_rate_limit(msg_resp, attempt)
                        continue
                    if msg_resp.status_code in {400, 403}:
                        captcha_payload, err = self._solve_captcha_payload(msg_resp)
                        if not captcha_payload:
                            return False, err
                        payload = {"content": final_msg}
                        payload.update(captcha_payload)
                        start = time.monotonic()
                        retry_resp = client.post(msg_url, json=payload)
                        self._record_request(time.monotonic() - start, retry_resp)
                        if retry_resp.status_code == 401:
                            token, refreshed = self._handle_unauthorized(client, account_id, token, refreshed)
                            if token:
                                continue
                            return False, "Unauthorized (token)"
                        if retry_resp.status_code == 200:
                            return True, "Success"
                        if retry_resp.status_code == 429:
                            self._wait_for_rate_limit(retry_resp, attempt)
                            continue
                        return False, f"Captcha error: {retry_resp.status_code}"
                    return False, f"Code: {msg_resp.status_code}"

                return False, "Rate Limit (message)"
            except Exception as e:
                return False, str(e)

    def run_mission(
        self,
        message_templates,
        delay_min,
        delay_max,
        use_friend_req=False,
        friend_delay_min=0,
        friend_delay_max=0,
        account_min_interval_seconds=0,
        target_min_interval_seconds=0,
        dry_run=False,
    ):
        self.is_running = True
        self._last_template = None
        self.log("[Mission] Starting...")
        self.db.reset_daily_counters()
        
        while self.is_running:
            accounts = self.db.get_active_accounts("discord")
            if not accounts: break

            did_send_attempt = False
            for acc in accounts:
                if not self.is_running: break
                acc_id, _, token, proxy, _, limit, sent_today, _, _, _, _ = acc
                
                if sent_today >= limit: continue

                if account_min_interval_seconds > 0:
                    remaining = self.db.get_account_dm_cooldown(acc_id, account_min_interval_seconds)
                    if remaining > 0:
                        self.log(f"[Mission] Account {acc_id}: waiting {remaining:.1f}s (cooldown).")
                        self._sleep_with_stop(remaining)
                        if not self.is_running:
                            break

                target = self.db.get_next_target("discord", min_target_interval_seconds=target_min_interval_seconds)
                if not target:
                    self.log("[System] No targets in database.")
                    self.is_running = False
                    return

                t_id, u_id = target
                did_send_attempt = True
                chosen_template = self._pick_template(message_templates)
                success, msg = self.send_dm(
                    acc_id,
                    token,
                    u_id,
                    chosen_template,
                    proxy,
                    use_friend_req,
                    friend_delay_min,
                    friend_delay_max,
                    dry_run,
                )
                if dry_run:
                    self.db.update_target_status(t_id, "Dry-Run")
                    self.db.increment_sent_counter(acc_id)
                    self.db.record_last_dm(acc_id, u_id)
                    preview = msg.replace("\n", " ")[:160]
                    suffix = "..." if len(msg) > 160 else ""
                    self.log(f"[Dry-Run] Would DM {u_id}: {preview}{suffix}")
                elif success:
                    self.db.update_target_status(t_id, "Sent")
                    self.db.increment_sent_counter(acc_id)
                    self.db.record_last_dm(acc_id, u_id)
                    self.log(f"[OK] DM sent to {u_id}")
                else:
                    self.db.update_target_status(t_id, "Failed", msg)
                    self.log(f"[!] Error {u_id}: {msg}")

                self._sleep_with_stop(random.randint(delay_min, delay_max))

            if self.is_running and not did_send_attempt:
                self.log("[Mission] All accounts reached the daily limit. Sleeping before next attempt.")
                self._sleep_with_stop(5)

    def stop(self):
        self.is_running = False

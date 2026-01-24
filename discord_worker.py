import httpx
import time
import random
import re
from collections import deque
from datetime import datetime, timedelta
from proxy_utils import httpx_client
from super_properties import set_super_properties_header
from delay_utils import gaussian_delay
from client_identity import USER_AGENT

class DiscordWorker:
    def __init__(self, db_manager, log_callback, metrics=None, captcha_solver=None, telemetry=None, gateway_manager=None):
        self.db = db_manager
        self.log = log_callback
        self.metrics = metrics
        self.captcha_solver = captcha_solver
        self.telemetry = telemetry
        self.gateway_manager = gateway_manager
        self.is_running = False
        self.max_retries = 3
        self.backoff_factor = 1.5
        self.default_tags = ["#promo", "#info", "#discord", "#community", "#support"]
        self.default_emojis = ["🔥", "✨", "✅", "🚀", "🎉", "💬", "🧩", "🌟"]
        self._last_template = None
        self._recent_templates = deque(maxlen=3)
        self.captcha_retry_base_seconds = 60
        self.captcha_retry_max_seconds = 900
        self.max_captcha_retries = 3
        self.dm_warmup_hours = self._get_dm_warmup_hours()

    def _get_dm_warmup_hours(self):
        try:
            raw = self.db.get_setting("dm_warmup_hours", "")
        except Exception:
            raw = ""
        if raw in (None, ""):
            return 48.0
        try:
            value = float(raw)
        except (TypeError, ValueError):
            return 48.0
        return max(0.0, value)

    def _is_dm_warmup_blocked(self, account_id):
        if self.dm_warmup_hours <= 0:
            return False, None
        age_hours = self.db.get_account_age_hours(account_id)
        if age_hours is None:
            return False, None
        if age_hours < self.dm_warmup_hours:
            return True, self.dm_warmup_hours - age_hours
        return False, None

    def _ensure_gateway_connected(self, token):
        if not self.gateway_manager:
            return True
        try:
            connected = self.gateway_manager.is_connected(token)
        except Exception:
            connected = False
        if not connected:
            self.log("[Gateway] WebSocket down; stopping HTTP sending.")
            self.is_running = False
            return False
        return True

    def _clear_token_cookies(self, token):
        try:
            self.db.clear_token_cookies(token)
        except Exception:
            pass

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
            self._recent_templates.clear()
            self._recent_templates.append(chosen)
            return chosen
        recent_set = set(self._recent_templates)
        candidates = [tpl for tpl in templates if tpl not in recent_set]
        if not candidates:
            candidates = templates
        chosen = random.choice(candidates)
        self._last_template = chosen
        self._recent_templates.append(chosen)
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
                if resp.status_code == 403:
                    self._clear_token_cookies(client.headers.get("Authorization"))
                captcha_payload, err, user_agent = self._solve_captcha_payload(resp)
                if not captcha_payload:
                    self.log(f"[Captcha] Friend request blocked for {user_id}: {err}")
                    return False
                if user_agent:
                    client.headers["User-Agent"] = user_agent
                    set_super_properties_header(client.headers, self.db, user_agent=user_agent)
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

    def _typing_delay_seconds(self, message, min_chars_per_sec=5.0, max_chars_per_sec=8.0):
        if not message:
            return 0.0
        length = len(message)
        if length <= 0:
            return 0.0
        cps = random.uniform(min_chars_per_sec, max_chars_per_sec)
        return max(0.2, length / max(0.1, cps))

    def _post_ack(self, client, channel_id=None, message_id=None, guild_id=None):
        url = None
        if channel_id and message_id:
            url = f"https://discord.com/api/v9/channels/{channel_id}/messages/{message_id}/ack"
        elif channel_id:
            url = f"https://discord.com/api/v9/channels/{channel_id}/ack"
        elif guild_id:
            url = f"https://discord.com/api/v9/guilds/{guild_id}/ack"
        if not url:
            return False
        try:
            client.post(url, json={"token": None})
            return True
        except Exception:
            return False

    def _prefetch_channel_messages(self, client, channel_id, limit=50):
        if not channel_id:
            return None
        url = f"https://discord.com/api/v9/channels/{channel_id}/messages"
        try:
            start = time.monotonic()
            resp = client.get(url, params={"limit": int(limit)})
            self._record_request(time.monotonic() - start, resp)
        except Exception:
            return None
        if resp.status_code != 200:
            return None
        try:
            return resp.json()
        except Exception:
            return None

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
            self._clear_token_cookies(current_token)
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
        if not isinstance(data, dict):
            return None
        def _get_captcha_field(payload, *keys):
            for key in keys:
                value = payload.get(key)
                if value not in (None, ""):
                    return value
            for container_key in ("captcha", "captcha_data", "captcha_payload", "captchaData"):
                nested = payload.get(container_key)
                if isinstance(nested, dict):
                    for key in keys:
                        value = nested.get(key)
                        if value not in (None, ""):
                            return value
            return None
        sitekey = _get_captcha_field(data, "captcha_sitekey", "sitekey", "captchaSitekey")
        if not sitekey:
            return None
        service = _get_captcha_field(data, "captcha_service") or "hcaptcha"
        api_server = _get_captcha_field(data, "captcha_api_server", "captcha_service_url")
        surl = _get_captcha_field(data, "captcha_surl") or api_server
        action = _get_captcha_field(data, "captcha_action")
        min_score = _get_captcha_field(data, "captcha_min_score", "captcha_score")
        data_s = _get_captcha_field(data, "captcha_data_s", "captcha_data-s", "captcha_s")
        cdata = _get_captcha_field(data, "captcha_cdata", "captcha_data")
        pagedata = _get_captcha_field(data, "captcha_pagedata", "captcha_chl_page_data")
        invisible = _get_captcha_field(data, "captcha_invisible")
        enterprise = _get_captcha_field(data, "captcha_enterprise")
        rqdata = _get_captcha_field(data, "captcha_rqdata", "rqdata")
        rqtoken = _get_captcha_field(data, "captcha_rqtoken", "rqtoken")
        return {
            "service": service,
            "sitekey": sitekey,
            "rqdata": rqdata,
            "rqtoken": rqtoken,
            "surl": surl,
            "api_server": api_server,
            "action": action,
            "min_score": min_score,
            "data_s": data_s,
            "cdata": cdata,
            "pagedata": pagedata,
            "invisible": invisible,
            "enterprise": enterprise,
            "url": "https://discord.com",
        }

    @staticmethod
    def _normalize_captcha_result(token_or_err):
        if isinstance(token_or_err, dict):
            token = (
                token_or_err.get("token")
                or token_or_err.get("gRecaptchaResponse")
                or token_or_err.get("response")
            )
            user_agent = token_or_err.get("userAgent") or token_or_err.get("useragent")
            return token, user_agent
        return token_or_err, None

    def _solve_captcha_payload(self, response):
        captcha_info = self._extract_captcha(response)
        if not captcha_info:
            return None, "Captcha required.", None
        if not self.captcha_solver:
            return None, "Captcha solver not configured.", None
        solved, token_or_err = self.captcha_solver.solve_captcha(captcha_info)
        if not solved:
            return None, f"Captcha error: {token_or_err}", None
        token, user_agent = self._normalize_captcha_result(token_or_err)
        if not token:
            return None, "Missing captcha token in response.", None
        payload = {"captcha_key": token}
        if captcha_info.get("rqtoken"):
            payload["captcha_rqtoken"] = captcha_info["rqtoken"]
        return payload, None, user_agent

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
                    delay = gaussian_delay(friend_delay_min, friend_delay_max)
                    if delay > 0:
                        self.log(f"[Dry-Run] Waiting {int(delay)}s before DM to {user_id}.")
                        self._sleep_with_stop(delay)
            return True, final_msg
        user_agent = USER_AGENT
        headers = {
            "Authorization": token,
            "Content-Type": "application/json",
            "User-Agent": user_agent
        }
        set_super_properties_header(headers, self.db)
        with httpx_client(
            proxy,
            headers=headers,
            timeout=httpx.Timeout(10.0),
            cookie_db=self.db,
            cookie_token=lambda: token,
        ) as client:
            try:
                if not self._ensure_gateway_connected(token):
                    return False, "Gateway offline"
                refreshed = False
                # Optional: send friend request.
                if add_friend:
                    self.send_friend_request(client, user_id)
                    if friend_delay_max > 0:
                        delay = gaussian_delay(friend_delay_min, friend_delay_max)
                        if delay > 0:
                            self.log(f"[Friend Request] Waiting {int(delay)}s before DM to {user_id}.")
                            self._sleep_with_stop(delay)

                # Open DM channel.
                url_channel = "https://discord.com/api/v9/users/@me/channels"
                channel_id = None
                for attempt in range(self.max_retries + 1):
                    if not self._ensure_gateway_connected(token):
                        return False, "Gateway offline"
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
                        if response.status_code == 403:
                            self._clear_token_cookies(token)
                        captcha_payload, err, user_agent = self._solve_captcha_payload(response)
                        if not captcha_payload:
                            return False, err
                        if user_agent:
                            client.headers["User-Agent"] = user_agent
                            set_super_properties_header(client.headers, self.db, user_agent=user_agent)
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
                typing_url = f"https://discord.com/api/v9/channels/{channel_id}/typing"

                # Message randomization (templates + spintax).
                final_msg = self.render_message(message_template)

                # Prefetch last messages + ack before sending DM.
                self._prefetch_channel_messages(client, channel_id, limit=50)
                self._post_ack(client, channel_id=channel_id)

                # Trigger typing indicator then wait a bit before sending.
                try:
                    start = time.monotonic()
                    typing_resp = client.post(typing_url)
                    self._record_request(time.monotonic() - start, typing_resp)
                except Exception:
                    pass
                self._sleep_with_stop(self._typing_delay_seconds(final_msg))

                for attempt in range(self.max_retries + 1):
                    if not self._ensure_gateway_connected(token):
                        return False, "Gateway offline"
                    start = time.monotonic()
                    msg_resp = client.post(msg_url, json={"content": final_msg})
                    self._record_request(time.monotonic() - start, msg_resp)
                    if msg_resp.status_code == 401:
                        token, refreshed = self._handle_unauthorized(client, account_id, token, refreshed)
                        if token:
                            continue
                        return False, "Unauthorized (token)"
                    if msg_resp.status_code == 200:
                        try:
                            msg_id = msg_resp.json().get("id")
                        except Exception:
                            msg_id = None
                        if msg_id:
                            self._post_ack(client, channel_id=channel_id, message_id=msg_id)
                        if self.telemetry:
                            self.telemetry.send_science(
                                token,
                                user_agent,
                                "dm_sent",
                                properties={"user_id": user_id, "channel_id": channel_id},
                                proxy=proxy,
                            )
                        return True, "Success"
                    if msg_resp.status_code == 429:
                        self._wait_for_rate_limit(msg_resp, attempt)
                        continue
                    if msg_resp.status_code in {400, 403}:
                        if msg_resp.status_code == 403:
                            self._clear_token_cookies(token)
                        captcha_payload, err, user_agent = self._solve_captcha_payload(msg_resp)
                        if not captcha_payload:
                            return False, err
                        if user_agent:
                            client.headers["User-Agent"] = user_agent
                            set_super_properties_header(client.headers, self.db, user_agent=user_agent)
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
                            try:
                                msg_id = retry_resp.json().get("id")
                            except Exception:
                                msg_id = None
                            if msg_id:
                                self._post_ack(client, channel_id=channel_id, message_id=msg_id)
                            if self.telemetry:
                                self.telemetry.send_science(
                                    token,
                                    user_agent,
                                    "dm_sent",
                                    properties={"user_id": user_id, "channel_id": channel_id},
                                    proxy=proxy,
                                )
                            return True, "Success"
                        if retry_resp.status_code == 429:
                            self._wait_for_rate_limit(retry_resp, attempt)
                            continue
                        return False, f"Captcha error: {retry_resp.status_code}"
                    return False, f"Code: {msg_resp.status_code}"

        return False, "Rate Limit (message)"

    def send_channel_message(
        self,
        account_id,
        token,
        channel_id,
        message_content,
        proxy=None,
    ):
        if not message_content:
            return False, "Empty message"
        if len(message_content) > 2000:
            message_content = message_content[:2000]
        user_agent = USER_AGENT
        headers = {
            "Authorization": token,
            "Content-Type": "application/json",
            "User-Agent": user_agent,
        }
        set_super_properties_header(headers, self.db)
        with httpx_client(
            proxy,
            headers=headers,
            timeout=httpx.Timeout(10.0),
            cookie_db=self.db,
            cookie_token=lambda: token,
        ) as client:
            try:
                if not self._ensure_gateway_connected(token):
                    return False, "Gateway offline"
                refreshed = False
                msg_url = f"https://discord.com/api/v9/channels/{channel_id}/messages"
                typing_url = f"https://discord.com/api/v9/channels/{channel_id}/typing"

                try:
                    start = time.monotonic()
                    typing_resp = client.post(typing_url)
                    self._record_request(time.monotonic() - start, typing_resp)
                except Exception:
                    pass
                self._sleep_with_stop(self._typing_delay_seconds(message_content))

                for attempt in range(self.max_retries + 1):
                    if not self._ensure_gateway_connected(token):
                        return False, "Gateway offline"
                    start = time.monotonic()
                    msg_resp = client.post(msg_url, json={"content": message_content})
                    self._record_request(time.monotonic() - start, msg_resp)
                    if msg_resp.status_code == 401:
                        token, refreshed = self._handle_unauthorized(
                            client, account_id, token, refreshed
                        )
                        if token:
                            continue
                        return False, "Unauthorized (token)"
                    if msg_resp.status_code == 200:
                        try:
                            msg_id = msg_resp.json().get("id")
                        except Exception:
                            msg_id = None
                        if msg_id:
                            self._post_ack(client, channel_id=channel_id, message_id=msg_id)
                        return True, "Success"
                    if msg_resp.status_code == 429:
                        self._wait_for_rate_limit(msg_resp, attempt)
                        continue
                    if msg_resp.status_code in {400, 403}:
                        if msg_resp.status_code == 403:
                            self._clear_token_cookies(token)
                        captcha_payload, err, user_agent = self._solve_captcha_payload(msg_resp)
                        if not captcha_payload:
                            return False, err
                        if user_agent:
                            client.headers["User-Agent"] = user_agent
                            set_super_properties_header(client.headers, self.db, user_agent=user_agent)
                        payload = {"content": message_content}
                        payload.update(captcha_payload)
                        start = time.monotonic()
                        retry_resp = client.post(msg_url, json=payload)
                        self._record_request(time.monotonic() - start, retry_resp)
                        if retry_resp.status_code == 401:
                            token, refreshed = self._handle_unauthorized(
                                client, account_id, token, refreshed
                            )
                            if token:
                                continue
                            return False, "Unauthorized (token)"
                        if retry_resp.status_code == 200:
                            try:
                                msg_id = retry_resp.json().get("id")
                            except Exception:
                                msg_id = None
                            if msg_id:
                                self._post_ack(client, channel_id=channel_id, message_id=msg_id)
                            return True, "Success (captcha)"
                        if retry_resp.status_code == 429:
                            self._wait_for_rate_limit(retry_resp, attempt)
                            continue
                        return False, f"Post-captcha error: {retry_resp.status_code}"
                    return False, f"Message error: {msg_resp.status_code}"
            except Exception as exc:
                return False, str(exc)
        return False, "Rate Limit (message)"
            except Exception as e:
                return False, str(e)

    def _is_captcha_error(self, message):
        return "captcha" in (message or "").lower()

    def _schedule_captcha_retry(self, target_id, user_id, error_msg):
        current = self.db.get_target_retry_count(target_id)
        if current >= self.max_captcha_retries:
            self.db.update_target_status(target_id, "Failed", error_msg)
            self.log(f"[Captcha] Target {user_id}: max retries reached. Marked Failed.")
            return
        delay = min(self.captcha_retry_max_seconds, self.captcha_retry_base_seconds * (2 ** current))
        retry_at = datetime.now() + timedelta(seconds=delay)
        self.db.set_target_retry(target_id, retry_at.strftime("%Y-%m-%d %H:%M:%S"), error_msg)
        self.log(f"[Captcha] Target {user_id}: retry scheduled in {int(delay)}s.")

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
        allowed_account_ids=None,
    ):
        self.is_running = True
        self._last_template = None
        self._recent_templates.clear()
        self.log("[Mission] Starting...")
        self.db.reset_daily_counters()
        
        while self.is_running:
            accounts = self.db.get_active_accounts("discord")
            if allowed_account_ids:
                allowed_set = set(allowed_account_ids)
                accounts = [acc for acc in accounts if acc[0] in allowed_set]
            if not accounts: break

            did_send_attempt = False
            for acc in accounts:
                if not self.is_running: break
                acc_id, _, token, proxy, _, limit, sent_today, _, _, _, _ = acc
                
                if sent_today >= limit: continue
                blocked, remaining_hours = self._is_dm_warmup_blocked(acc_id)
                if blocked:
                    self.log(
                        f"[Warmup] Account {acc_id}: DM blocked for {remaining_hours:.1f}h (new account)."
                    )
                    continue

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
                if self._is_captcha_error(msg):
                    self._schedule_captcha_retry(t_id, u_id, msg)
                else:
                    self.db.update_target_status(t_id, "Failed", msg)
                    self.log(f"[!] Error {u_id}: {msg}")

                self._sleep_with_stop(gaussian_delay(delay_min, delay_max))

            if self.is_running and not did_send_attempt:
                self.log("[Mission] All accounts reached the daily limit. Sleeping before next attempt.")
                self._sleep_with_stop(5)

    def stop(self):
        self.is_running = False

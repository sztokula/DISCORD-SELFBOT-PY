import httpx
from proxy_utils import httpx_client
import time
import random
import uuid
from super_properties import set_super_properties_header
from delay_utils import gaussian_delay
from client_identity import USER_AGENT

class DiscordJoiner:
    def __init__(self, db_manager, log_callback, captcha_solver=None, metrics=None, telemetry=None):
        self.db = db_manager
        self.log = log_callback
        self.is_running = False
        self.captcha_solver = captcha_solver
        self.metrics = metrics
        self.telemetry = telemetry
        self.max_retries = 3
        self.backoff_factor = 1.5
        self.captcha_retry_base_seconds = 60
        self.captcha_retry_max_seconds = 900
        self.max_captcha_retries = 3

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
        if current_token:
            try:
                self.db.clear_token_cookies(current_token)
            except Exception:
                pass
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

    def join_server(
        self,
        account_id,
        token,
        invite_code,
        proxy=None,
        auto_accept_rules=True,
        auto_onboarding=True,
        role_whitelist=None,
        auto_verify_button=False,
        verification_channel_id=None,
    ):
        """invite_code: only the invite slug, e.g. 'cool-server' from discord.gg/cool-server"""
        # Strip invite code in case a full link is pasted.
        invite_code = invite_code.split("/")[-1]
        
        url = f"https://discord.com/api/v9/invites/{invite_code}"
        user_agent = USER_AGENT
        headers = {
            "Authorization": token,
            "Content-Type": "application/json",
            "User-Agent": user_agent
        }
        set_super_properties_header(headers, self.db)
        
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
                    response = client.post(url, json={})
                    self._record_request(time.monotonic() - start, response)
                    if response.status_code == 401:
                        token, refreshed = self._handle_unauthorized(client, account_id, token, refreshed)
                        if token:
                            continue
                        return False, "Unauthorized (token)", None
                    if response.status_code == 200:
                        guild_id = self._extract_guild_id(response)
                        if guild_id:
                            self._browse_guild(client, guild_id)
                        if guild_id:
                            self._post_ack(client, guild_id=guild_id)
                        if self.telemetry:
                            self.telemetry.send_science(
                                token,
                                user_agent,
                                "join_server",
                                properties={"invite": invite_code, "guild_id": guild_id},
                                proxy=proxy,
                            )
                        self._handle_post_join(
                            client,
                            guild_id,
                            auto_accept_rules,
                            auto_onboarding,
                            role_whitelist,
                            auto_verify_button,
                            verification_channel_id,
                        )
                        return True, "Success", guild_id
                    if response.status_code in {400, 403}:
                        if response.status_code == 403:
                            try:
                                self.db.clear_token_cookies(token)
                            except Exception:
                                pass
                        captcha_payload, err, user_agent = self._solve_captcha_payload(response)
                        if captcha_payload:
                            if user_agent:
                                client.headers["User-Agent"] = user_agent
                                set_super_properties_header(client.headers, self.db, user_agent=user_agent)
                            start = time.monotonic()
                            retry_resp = client.post(url, json=captcha_payload)
                            self._record_request(time.monotonic() - start, retry_resp)
                            if retry_resp.status_code == 200:
                                guild_id = self._extract_guild_id(retry_resp)
                                if guild_id:
                                    self._browse_guild(client, guild_id)
                                if guild_id:
                                    self._post_ack(client, guild_id=guild_id)
                                if self.telemetry:
                                    self.telemetry.send_science(
                                        token,
                                        user_agent,
                                        "join_server",
                                        properties={"invite": invite_code, "guild_id": guild_id},
                                        proxy=proxy,
                                    )
                                self._handle_post_join(
                                    client,
                                    guild_id,
                                    auto_accept_rules,
                                    auto_onboarding,
                                    role_whitelist,
                                    auto_verify_button,
                                    verification_channel_id,
                                )
                                return True, "Success (captcha)", guild_id
                            return False, f"Post-captcha error: {retry_resp.status_code}", None
                        if err:
                            return False, err, None
                        return False, "Verification required (Captcha/Phone)", None
                    if response.status_code == 429:
                        retry_after = self._get_retry_after(response)
                        wait_time = retry_after * (self.backoff_factor ** attempt)
                        self._sleep_with_stop(wait_time)
                        continue
                    return False, f"Error {response.status_code}", None
        except Exception as e:
            return False, str(e), None

        return False, "Rate Limit (after retries)", None

    def run_mass_join(
        self,
        invite_codes,
        delay_min,
        delay_max,
        on_complete=None,
        auto_accept_rules=True,
        auto_onboarding=True,
        role_whitelist=None,
        auto_verify_button=False,
        verification_channel_id=None,
        allowed_account_ids=None,
    ):
        self.is_running = True
        self.db.reset_daily_counters()
        accounts = self.db.get_active_accounts("discord")
        if allowed_account_ids:
            allowed_set = set(allowed_account_ids)
            accounts = [acc for acc in accounts if acc[0] in allowed_set]
        joined_any = False
        pending_retries = []
        
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
        join_counts = {}
        join_limits = {}
        for acc in accounts:
            acc_id, _, _, _, _, _, _, _, join_limit, join_today, _ = acc
            join_counts[acc_id] = join_today
            join_limits[acc_id] = join_limit

        for acc in accounts:
            if not self.is_running: break
            
            acc_id, _, token, proxy, _, _, _, _, join_limit, join_today, _ = acc
            if join_counts.get(acc_id, join_today) >= join_limit:
                self.log(f"[Joiner] Account {acc_id}: daily join limit reached ({join_today}/{join_limit}).")
                continue
            invite_code = random.choice(invite_codes)
            success, msg, guild_id = self.join_server(
                acc_id,
                token,
                invite_code,
                proxy,
                auto_accept_rules=auto_accept_rules,
                auto_onboarding=auto_onboarding,
                role_whitelist=role_whitelist,
                auto_verify_button=auto_verify_button,
                verification_channel_id=verification_channel_id,
            )
            
            if success:
                self.db.increment_join_counter(acc_id)
                join_counts[acc_id] = join_counts.get(acc_id, 0) + 1
                joined_any = True
                if guild_id:
                    self.log(f"[Joiner] Account {acc_id}: JOINED ({invite_code}, guild {guild_id}).")
                else:
                    self.log(f"[Joiner] Account {acc_id}: JOINED ({invite_code}).")
            else:
                if self._is_captcha_error(msg):
                    self._schedule_join_retry(
                        pending_retries,
                        acc_id,
                        token,
                        proxy,
                        invite_code,
                        0,
                        msg,
                    )
                else:
                    self.log(f"[Joiner] Account {acc_id}: ERROR ({msg}) [{invite_code}]")
            
            did_join_attempt = True
            # IMPORTANT: add a larger delay between joins.
            wait = gaussian_delay(delay_min, delay_max)
            self.log(f"[Joiner] Waiting {int(wait)}s before the next account...")
            self._sleep_with_stop(wait)

        while self.is_running and pending_retries:
            pending_retries.sort(key=lambda item: item["retry_at"])
            task = pending_retries.pop(0)
            acc_id = task["acc_id"]
            if join_counts.get(acc_id, 0) >= join_limits.get(acc_id, 0):
                self.log(f"[Joiner] Account {acc_id}: daily join limit reached (retry skipped).")
                continue
            wait_seconds = max(0.0, task["retry_at"] - time.monotonic())
            if wait_seconds > 0:
                self.log(f"[Joiner] Waiting {int(wait_seconds)}s for captcha retry...")
                self._sleep_with_stop(wait_seconds)
                if not self.is_running:
                    break
            success, msg, guild_id = self.join_server(
                acc_id,
                task["token"],
                task["invite_code"],
                task["proxy"],
                auto_accept_rules=auto_accept_rules,
                auto_onboarding=auto_onboarding,
                role_whitelist=role_whitelist,
                auto_verify_button=auto_verify_button,
                verification_channel_id=verification_channel_id,
            )
            if success:
                self.db.increment_join_counter(acc_id)
                join_counts[acc_id] = join_counts.get(acc_id, 0) + 1
                joined_any = True
                if guild_id:
                    self.log(f"[Joiner] Account {acc_id}: JOINED ({task['invite_code']}, guild {guild_id}).")
                else:
                    self.log(f"[Joiner] Account {acc_id}: JOINED ({task['invite_code']}).")
            else:
                if self._is_captcha_error(msg) and task["attempt"] < self.max_captcha_retries:
                    self._schedule_join_retry(
                        pending_retries,
                        acc_id,
                        task["token"],
                        task["proxy"],
                        task["invite_code"],
                        task["attempt"],
                        msg,
                    )
                else:
                    self.log(f"[Joiner] Account {acc_id}: ERROR ({msg}) [{task['invite_code']}]")
            wait = gaussian_delay(delay_min, delay_max)
            self.log(f"[Joiner] Waiting {int(wait)}s before the next retry...")
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

    def _browse_guild(self, client, guild_id, channels_to_visit=2, messages_limit=15):
        if not guild_id:
            return False
        try:
            start = time.monotonic()
            resp = client.get(f"https://discord.com/api/v9/guilds/{guild_id}/channels")
            self._record_request(time.monotonic() - start, resp)
        except Exception:
            return False
        if resp.status_code != 200:
            return False
        try:
            channels = resp.json()
        except Exception:
            return False
        if not isinstance(channels, list):
            return False
        candidates = []
        for ch in channels:
            if not isinstance(ch, dict):
                continue
            ch_type = ch.get("type")
            if ch_type in (0, 5, 15):
                ch_id = ch.get("id")
                if ch_id:
                    candidates.append(ch_id)
        if not candidates:
            return False
        sample_count = min(max(1, int(channels_to_visit)), len(candidates))
        picked = random.sample(candidates, k=sample_count)
        for idx, channel_id in enumerate(picked):
            try:
                start = time.monotonic()
                resp = client.get(
                    f"https://discord.com/api/v9/channels/{channel_id}/messages",
                    params={"limit": int(messages_limit)},
                )
                self._record_request(time.monotonic() - start, resp)
            except Exception:
                continue
            if resp.status_code in (200, 204):
                msg_id = None
                try:
                    payload = resp.json()
                    if isinstance(payload, list) and payload:
                        msg_id = payload[0].get("id")
                except Exception:
                    msg_id = None
                if msg_id:
                    self._post_ack(client, channel_id=channel_id, message_id=msg_id)
                else:
                    self._post_ack(client, channel_id=channel_id)
            if idx < len(picked) - 1:
                self._sleep_with_stop(gaussian_delay(1.0, 3.0))
        return True

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

    def _is_captcha_error(self, message):
        lowered = (message or "").lower()
        return "captcha" in lowered and "phone" not in lowered

    def _schedule_join_retry(self, pending_retries, acc_id, token, proxy, invite_code, attempt, error_msg):
        if attempt >= self.max_captcha_retries:
            self.log(f"[Captcha] Account {acc_id}: max retries reached ({invite_code}).")
            return
        delay = min(self.captcha_retry_max_seconds, self.captcha_retry_base_seconds * (2 ** attempt))
        pending_retries.append(
            {
                "acc_id": acc_id,
                "token": token,
                "proxy": proxy,
                "invite_code": invite_code,
                "attempt": attempt + 1,
                "retry_at": time.monotonic() + delay,
            }
        )
        self.log(f"[Captcha] Account {acc_id}: retry scheduled in {int(delay)}s ({invite_code}).")

    def _extract_guild_id(self, response):
        try:
            data = response.json()
        except Exception:
            return None
        if isinstance(data, dict):
            guild = data.get("guild") or {}
            return data.get("guild_id") or guild.get("id")
        return None

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

    def _submit_with_captcha(self, client, method, url, payload):
        for attempt in range(self.max_retries + 1):
            start = time.monotonic()
            resp = client.request(method, url, json=payload)
            self._record_request(time.monotonic() - start, resp)
            if resp.status_code in {200, 204}:
                return True
            if resp.status_code == 429:
                retry_after = self._get_retry_after(resp)
                wait_time = retry_after * (self.backoff_factor ** attempt)
                self._sleep_with_stop(wait_time)
                continue
            if resp.status_code in {400, 403}:
                captcha_payload, err, user_agent = self._solve_captcha_payload(resp)
                if not captcha_payload:
                    self.log(f"[Captcha] Failed: {err}")
                    return False
                if user_agent:
                    client.headers["User-Agent"] = user_agent
                    set_super_properties_header(client.headers, self.db, user_agent=user_agent)
                retry_payload = dict(payload)
                retry_payload.update(captcha_payload)
                start = time.monotonic()
                retry_resp = client.request(method, url, json=retry_payload)
                self._record_request(time.monotonic() - start, retry_resp)
                if retry_resp.status_code in {200, 204}:
                    return True
                if retry_resp.status_code == 429:
                    retry_after = self._get_retry_after(retry_resp)
                    wait_time = retry_after * (self.backoff_factor ** attempt)
                    self._sleep_with_stop(wait_time)
                    continue
                self.log(f"[Captcha] Post-captcha error: {retry_resp.status_code}")
                return False
            self.log(f"[Joiner] Request failed ({resp.status_code}).")
            return False
        return False

    def _fetch_channel_messages(self, client, channel_id, limit=50):
        if not channel_id:
            return None
        url = f"https://discord.com/api/v9/channels/{channel_id}/messages"
        for attempt in range(self.max_retries + 1):
            try:
                start = time.monotonic()
                resp = client.get(url, params={"limit": int(limit)})
                self._record_request(time.monotonic() - start, resp)
            except Exception:
                return None
            if resp.status_code == 200:
                try:
                    return resp.json()
                except Exception:
                    return None
            if resp.status_code == 429:
                retry_after = self._get_retry_after(resp)
                wait_time = retry_after * (self.backoff_factor ** attempt)
                self._sleep_with_stop(wait_time)
                continue
            if resp.status_code in {401, 403, 404}:
                self.log(f"[Joiner] Cannot read verification channel ({resp.status_code}).")
                return None
            return None
        return None

    def _extract_button_custom_id(self, message):
        if not isinstance(message, dict):
            return None
        components = message.get("components") or []
        for row in components:
            if isinstance(row, dict):
                row_components = row.get("components") or []
            elif isinstance(row, list):
                row_components = row
            else:
                continue
            for comp in row_components:
                if not isinstance(comp, dict):
                    continue
                if comp.get("type") != 2:
                    continue
                if comp.get("disabled"):
                    continue
                custom_id = comp.get("custom_id")
                if custom_id:
                    return custom_id
        return None

    def _find_verification_button(self, messages):
        if not isinstance(messages, list):
            return None, None
        for msg in messages:
            custom_id = self._extract_button_custom_id(msg)
            if custom_id:
                return msg, custom_id
        return None, None

    def _send_button_interaction(self, client, guild_id, channel_id, message_id, application_id, custom_id):
        if not (channel_id and message_id and application_id and custom_id):
            return False
        payload = {
            "type": 3,
            "channel_id": channel_id,
            "message_id": message_id,
            "application_id": application_id,
            "data": {
                "component_type": 2,
                "custom_id": custom_id,
            },
            "session_id": uuid.uuid4().hex,
        }
        if guild_id:
            payload["guild_id"] = guild_id
        return self._submit_with_captcha(
            client,
            "POST",
            "https://discord.com/api/v9/interactions",
            payload,
        )

    def _click_verification_button(self, client, guild_id, channel_id):
        self.log(f"[Debug] Attempting verification click (guild={guild_id}, channel={channel_id}).")
        messages = self._fetch_channel_messages(client, channel_id, limit=50)
        msg, custom_id = self._find_verification_button(messages)
        if not msg or not custom_id:
            self.log("[Joiner] Verification button not found in channel.")
            return False
        message_id = msg.get("id")
        application_id = msg.get("application_id")
        if not application_id:
            author = msg.get("author") or {}
            application_id = author.get("id")
        if not (message_id and application_id):
            self.log("[Joiner] Verification message missing IDs.")
            return False
        ok = self._send_button_interaction(
            client,
            guild_id,
            channel_id,
            message_id,
            application_id,
            custom_id,
        )
        if ok:
            self.log("[Joiner] Verification button clicked.")
        return ok

    def _build_rule_response(self, field):
        choices = field.get("choices")
        if not choices:
            choices = field.get("values")
        if isinstance(choices, list) and choices:
            values = []
            for choice in choices:
                if isinstance(choice, dict):
                    value = choice.get("value") or choice.get("id") or choice.get("label")
                else:
                    value = choice
                if value is not None:
                    values.append(value)
            if values:
                return values
        return True

    def _accept_rules(self, client, guild_id):
        self.log(f"[Debug] Accepting rules for guild {guild_id}.")
        url = f"https://discord.com/api/v9/guilds/{guild_id}/member-verification?with_guild=false"
        start = time.monotonic()
        response = client.get(url)
        self._record_request(time.monotonic() - start, response)
        if response.status_code == 404:
            return True
        if response.status_code != 200:
            self.log(f"[Joiner] Rules fetch failed ({response.status_code}).")
            return False
        try:
            data = response.json()
        except Exception:
            self.log("[Joiner] Rules response parse failed.")
            return False
        form_fields = data.get("form_fields") or []
        if not form_fields:
            return True
        for field in form_fields:
            field["response"] = self._build_rule_response(field)
        payload = {
            "version": data.get("version"),
            "form_fields": form_fields,
        }
        ok = self._submit_with_captcha(
            client,
            "PUT",
            f"https://discord.com/api/v9/guilds/{guild_id}/requests/@me",
            payload,
        )
        if ok:
            self.log(f"[Joiner] Accepted rules for guild {guild_id}.")
        return ok

    def _complete_onboarding(self, client, guild_id, role_whitelist=None):
        self.log(f"[Debug] Completing onboarding for guild {guild_id}.")
        url = f"https://discord.com/api/v9/guilds/{guild_id}/onboarding"
        start = time.monotonic()
        response = client.get(url)
        self._record_request(time.monotonic() - start, response)
        if response.status_code == 404:
            return True
        if response.status_code != 200:
            self.log(f"[Joiner] Onboarding fetch failed ({response.status_code}).")
            return False
        try:
            data = response.json()
        except Exception:
            self.log("[Joiner] Onboarding response parse failed.")
            return False
        prompts = data.get("prompts") or []
        if not prompts:
            return True
        whitelist = {str(role_id) for role_id in (role_whitelist or [])}
        responses = []
        for prompt in prompts:
            options = prompt.get("options") or []
            if whitelist:
                option_ids = []
                for opt in options:
                    role_ids = opt.get("role_ids") or []
                    if any(str(role_id) in whitelist for role_id in role_ids):
                        opt_id = opt.get("id")
                        if opt_id:
                            option_ids.append(opt_id)
                if not option_ids:
                    option_ids = [opt.get("id") for opt in options if opt.get("id")]
                    if option_ids:
                        self.log(
                            f"[Joiner] No whitelist match for prompt {prompt.get('id')}. Using fallback options."
                        )
            else:
                option_ids = [opt.get("id") for opt in options if opt.get("id")]
            max_options = prompt.get("max_options")
            if max_options is None and prompt.get("single_select"):
                max_options = 1
            if max_options is not None and len(option_ids) > max_options:
                option_ids = option_ids[:max_options]
            responses.append(
                {
                    "prompt_id": prompt.get("id"),
                    "option_ids": option_ids,
                }
            )
        payload = {"onboarding_responses": responses, "guild_id": guild_id}
        ok = self._submit_with_captcha(
            client,
            "POST",
            f"https://discord.com/api/v9/guilds/{guild_id}/onboarding-responses",
            payload,
        )
        if ok:
            self.log(f"[Joiner] Completed onboarding for guild {guild_id}.")
        return ok

    def _handle_post_join(
        self,
        client,
        guild_id,
        auto_accept_rules,
        auto_onboarding,
        role_whitelist=None,
        auto_verify_button=False,
        verification_channel_id=None,
    ):
        if not guild_id:
            self.log("[Joiner] Guild id missing; skipping rules/onboarding.")
            return
        if auto_accept_rules:
            self._accept_rules(client, guild_id)
        if auto_onboarding:
            self._complete_onboarding(client, guild_id, role_whitelist)
        if auto_verify_button and verification_channel_id:
            self._click_verification_button(client, guild_id, verification_channel_id)

import httpx
import time
import random
import re

class DiscordWorker:
    def __init__(self, db_manager, log_callback):
        self.db = db_manager
        self.log = log_callback
        self.is_running = False
        self.max_retries = 3
        self.backoff_factor = 1.5
        self.default_tags = ["#promo", "#info", "#discord", "#community", "#support"]
        self.default_emojis = ["🔥", "✨", "✅", "🚀", "🎉", "💬", "🧩", "🌟"]

    def parse_spintax(self, text):
        """Zamienia {opcja1|opcja2} na losową opcję."""
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

    def send_friend_request(self, client, user_id):
        """Opcjonalna funkcja wysyłania zaproszenia do znajomych."""
        url = f"https://discord.com/api/v9/users/{user_id}/relationships"
        try:
            resp = client.put(url, json={})
            return resp.status_code == 204
        except Exception as e:
            self.log(f"[Friend Request] Exception for user {user_id}: {e}")
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
            self.log(f"[Auth] Nie udało się odświeżyć tokenu dla konta {account_id}: {e}")
            return None
        if fresh_token and fresh_token != current_token:
            self.log(f"[Auth] Odświeżono token dla konta {account_id}.")
            return fresh_token
        return None

    def _handle_unauthorized(self, client, account_id, current_token, refreshed):
        if refreshed:
            self.log(f"[Auth] Token dla konta {account_id} nadal niepoprawny. Dezaktywuję konto.")
            if account_id is not None:
                self.db.update_account_status(account_id, "Banned/Dead")
                self.db.remove_account(account_id)
            return None, True
        new_token = self._refresh_token(account_id, current_token)
        if new_token:
            client.headers["Authorization"] = new_token
            return new_token, True
        self.log(f"[Auth] Brak nowego tokenu dla konta {account_id}. Dezaktywuję konto.")
        if account_id is not None:
            self.db.update_account_status(account_id, "Banned/Dead")
            self.db.remove_account(account_id)
        return None, True

    def send_dm(self, account_id, token, user_id, message_template, proxy=None, add_friend=False, friend_delay_min=0, friend_delay_max=0):
        headers = {
            "Authorization": token,
            "Content-Type": "application/json",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        }
        proxies = {"all://": proxy} if proxy else None
        
        with httpx.Client(proxies=proxies, headers=headers, timeout=httpx.Timeout(10.0)) as client:
            try:
                refreshed = False
                # Opcjonalnie: Zaproszenie do znajomych
                if add_friend:
                    self.send_friend_request(client, user_id)
                    if friend_delay_max > 0:
                        delay = random.randint(friend_delay_min, friend_delay_max)
                        if delay > 0:
                            self.log(f"[Friend Request] Oczekiwanie {delay}s przed DM do {user_id}.")
                            self._sleep_with_stop(delay)

                # Otwarcie kanału DM
                url_channel = "https://discord.com/api/v9/users/@me/channels"
                channel_id = None
                for attempt in range(self.max_retries + 1):
                    response = client.post(url_channel, json={"recipient_id": user_id})
                    if response.status_code == 401:
                        token, refreshed = self._handle_unauthorized(client, account_id, token, refreshed)
                        if token:
                            continue
                        return False, "Unauthorized (token)"
                    if response.status_code == 200:
                        channel_id = response.json()['id']
                        break
                    if response.status_code == 429:
                        self._wait_for_rate_limit(response, attempt)
                        continue
                    return False, f"Channel Error: {response.status_code}"

                if not channel_id:
                    return False, "Rate Limit (kanał DM)"

                msg_url = f"https://discord.com/api/v9/channels/{channel_id}/messages"

                # Losowanie wiadomości (szablony + spintax)
                final_msg = self.render_message(message_template)

                for attempt in range(self.max_retries + 1):
                    msg_resp = client.post(msg_url, json={"content": final_msg})
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
                    return False, f"Code: {msg_resp.status_code}"

                return False, "Rate Limit (wiadomość)"
            except Exception as e:
                return False, str(e)

    def run_mission(self, message_templates, delay_min, delay_max, use_friend_req=False, friend_delay_min=0, friend_delay_max=0):
        self.is_running = True
        self.log("[Mission] Startujemy...")
        self.db.reset_daily_counters()
        
        while self.is_running:
            accounts = self.db.get_active_accounts("discord")
            if not accounts: break

            did_send_attempt = False
            for acc in accounts:
                if not self.is_running: break
                acc_id, _, token, proxy, _, limit, sent_today, _, _, _, _ = acc
                
                if sent_today >= limit: continue

                target = self.db.get_next_target("discord")
                if not target:
                    self.log("[System] Brak celów w bazie.")
                    self.is_running = False
                    return

                t_id, u_id = target
                did_send_attempt = True
                chosen_template = random.choice(message_templates)
                success, msg = self.send_dm(
                    acc_id,
                    token,
                    u_id,
                    chosen_template,
                    proxy,
                    use_friend_req,
                    friend_delay_min,
                    friend_delay_max,
                )
                
                if success:
                    self.db.update_target_status(t_id, "Sent")
                    self.db.increment_sent_counter(acc_id)
                    self.log(f"[OK] DM wysłany do {u_id}")
                else:
                    self.db.update_target_status(t_id, "Failed", msg)
                    self.log(f"[!] Błąd {u_id}: {msg}")

                self._sleep_with_stop(random.randint(delay_min, delay_max))

            if self.is_running and not did_send_attempt:
                self.log("[Mission] Wszystkie konta mają dzienny limit. Uśpienie przed kolejną próbą.")
                self._sleep_with_stop(5)

    def stop(self):
        self.is_running = False

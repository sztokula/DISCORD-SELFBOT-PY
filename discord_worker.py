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

    def parse_spintax(self, text):
        """Zamienia {opcja1|opcja2} na losową opcję."""
        while True:
            match = re.search(r'\{([^{}]*)\}', text)
            if not match:
                break
            options = match.group(1).split('|')
            text = text.replace(match.group(0), random.choice(options), 1)
        return text

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

    def send_dm(self, token, user_id, message, proxy=None, add_friend=False):
        headers = {
            "Authorization": token,
            "Content-Type": "application/json",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        }
        proxies = {"all://": proxy} if proxy else None
        
        with httpx.Client(proxies=proxies, headers=headers, timeout=httpx.Timeout(10.0)) as client:
            try:
                # Opcjonalnie: Zaproszenie do znajomych
                if add_friend:
                    self.send_friend_request(client, user_id)
                    self._sleep_with_stop(random.randint(2, 5))

                # Otwarcie kanału DM
                url_channel = "https://discord.com/api/v9/users/@me/channels"
                channel_id = None
                for attempt in range(self.max_retries + 1):
                    response = client.post(url_channel, json={"recipient_id": user_id})
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

                # Losowanie wiadomości (Spintax)
                final_msg = self.parse_spintax(message)

                for attempt in range(self.max_retries + 1):
                    msg_resp = client.post(msg_url, json={"content": final_msg})
                    if msg_resp.status_code == 200:
                        return True, "Success"
                    if msg_resp.status_code == 429:
                        self._wait_for_rate_limit(msg_resp, attempt)
                        continue
                    return False, f"Code: {msg_resp.status_code}"

                return False, "Rate Limit (wiadomość)"
            except Exception as e:
                return False, str(e)

    def run_mission(self, message, delay_min, delay_max, use_friend_req=False):
        self.is_running = True
        self.log("[Mission] Startujemy...")
        self.db.reset_daily_counters()
        
        while self.is_running:
            accounts = self.db.get_active_accounts("discord")
            if not accounts: break

            did_send_attempt = False
            for acc in accounts:
                if not self.is_running: break
                acc_id, _, token, proxy, _, limit, sent_today, _ = acc
                
                if sent_today >= limit: continue

                target = self.db.get_next_target("discord")
                if not target:
                    self.log("[System] Brak celów w bazie.")
                    self.is_running = False
                    return

                t_id, u_id = target
                did_send_attempt = True
                success, msg = self.send_dm(token, u_id, message, proxy, use_friend_req)
                
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

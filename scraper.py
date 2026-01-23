import httpx
from proxy_utils import httpx_client
import time
from super_properties import set_super_properties_header

class DiscordScraper:
    def __init__(self, db_manager, log_callback, metrics=None):
        self.db = db_manager
        self.log = log_callback
        self.metrics = metrics
        self.is_scraping = False
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

    def _wait_for_rate_limit(self, response, attempt):
        retry_after = self._get_retry_after(response)
        wait_time = retry_after * (self.backoff_factor ** attempt)
        self._sleep_with_stop(wait_time)

    def _wait_for_bucket_reset(self, response, reason):
        remaining = response.headers.get("X-RateLimit-Remaining")
        reset_after = response.headers.get("X-RateLimit-Reset-After")
        if remaining != "0":
            return
        try:
            reset_value = float(reset_after)
        except (TypeError, ValueError):
            reset_value = None
        if reset_value is None:
            return
        self.log(f"[Scraper] Endpoint limit ({reason}) exhausted. Waiting {reset_value:.2f}s...")
        self._sleep_with_stop(reset_value)

    def _log_member_list_permission_error(self, response):
        message = None
        code = None
        try:
            payload = response.json()
            message = payload.get("message")
            code = payload.get("code")
        except Exception:
            payload = None
        detail_parts = []
        if code is not None:
            detail_parts.append(f"code={code}")
        if message:
            detail_parts.append(f"message={message}")
        details = f" ({', '.join(detail_parts)})" if detail_parts else ""
        self.log(
            "[Scraper] Missing permissions to fetch member list (HTTP 403). "
            "Check that the token has server access and required permissions." + details
        )

    def _sleep_with_stop(self, total_seconds, interval=0.5):
        end_time = time.monotonic() + max(0.0, total_seconds)
        while self.is_scraping and time.monotonic() < end_time:
            remaining = end_time - time.monotonic()
            time.sleep(min(interval, max(0.0, remaining)))

    def _fetch_self_id(self, client):
        start = time.monotonic()
        response = client.get("https://discord.com/api/v9/users/@me")
        self._record_request(time.monotonic() - start, response)
        if response.status_code != 200:
            self.log(f"[Scraper] Failed to fetch @me: {response.status_code}")
            return None
        try:
            data = response.json()
        except Exception:
            self.log("[Scraper] Failed to parse @me response.")
            return None
        return data.get("id")

    def scrape_history(self, token, channel_id, limit=1000, on_complete=None, proxy=None):
        """Fetch user IDs that have posted in a given channel."""
        self.is_scraping = True
        added_any = False
        self.log(f"[Scraper] Starting scrape from channel {channel_id}...")
        
        headers = {
            "Authorization": token,
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        }
        set_super_properties_header(headers, self.db)
        url = f"https://discord.com/api/v9/channels/{channel_id}/messages"
        
        unique_ids = set()
        last_msg_id = None
        rate_limit_attempt = 0
        
        try:
            with httpx_client(proxy, headers=headers, timeout=httpx.Timeout(10.0)) as client:
                self_id = self._fetch_self_id(client)
                while len(unique_ids) < limit and self.is_scraping:
                    params = {"limit": 100}
                    if last_msg_id:
                        params["before"] = last_msg_id
                    
                    start = time.monotonic()
                    response = client.get(url, params=params)
                    self._record_request(time.monotonic() - start, response)
                    
                    if response.status_code == 429:
                        if rate_limit_attempt >= self.max_retries:
                            self.log("[Scraper] Rate limit exceeded. Stopping.")
                            break
                        self.log("[Scraper] Rate limit. Applying backoff...")
                        self._wait_for_rate_limit(response, rate_limit_attempt)
                        rate_limit_attempt += 1
                        continue
                    if response.status_code != 200:
                        self.log(f"[Scraper] Error: {response.status_code}")
                        break

                    messages = response.json()
                    rate_limit_attempt = 0
                    if not messages:
                        break
                    
                    for msg in messages:
                        u_id = msg['author']['id']
                        # Skip bots and own account.
                        if not msg['author'].get('bot') and u_id != self_id:
                            unique_ids.add(u_id)
                        last_msg_id = msg['id']
                    
                    self.log(f"[Scraper] Found unique: {len(unique_ids)}...")
                    self._sleep_with_stop(1) # Delay to avoid rate limit.
            
            # Zapis do bazy
            if unique_ids:
                self.db.add_targets(list(unique_ids), "discord")
                added_any = True
                self.log(f"[Scraper] Success. Added {len(unique_ids)} new targets to the database.")
            
        except Exception as e:
            self.log(f"[Scraper] Critical error: {str(e)}")
        
        self.is_scraping = False
        if on_complete:
            on_complete(added_any)

    def stop(self):
        self.is_scraping = False

    def scrape_guild_members(self, token, guild_id, limit=1000, on_complete=None, proxy=None):
        """Fetch server member list via /guilds/{id}/members."""
        self.is_scraping = True
        added_any = False
        self.log(f"[Scraper] Starting member list fetch for guild {guild_id}...")

        headers = {
            "Authorization": token,
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        }
        set_super_properties_header(headers, self.db)
        url = f"https://discord.com/api/v9/guilds/{guild_id}/members"

        unique_ids = set()
        last_member_id = None
        rate_limit_attempt = 0

        try:
            with httpx_client(proxy, headers=headers, timeout=httpx.Timeout(10.0)) as client:
                self_id = self._fetch_self_id(client)
                while len(unique_ids) < limit and self.is_scraping:
                    remaining = max(1, limit - len(unique_ids))
                    params = {"limit": min(1000, remaining)}
                    if last_member_id:
                        params["after"] = last_member_id

                    start = time.monotonic()
                    response = client.get(url, params=params)
                    self._record_request(time.monotonic() - start, response)

                    if response.status_code == 429:
                        if rate_limit_attempt >= self.max_retries:
                            self.log("[Scraper] Rate limit exceeded. Stopping.")
                            break
                        scope = response.headers.get("X-RateLimit-Scope", "route")
                        scope_info = "global" if response.headers.get("X-RateLimit-Global") else scope
                        self.log(f"[Scraper] Rate limit for member list ({scope_info}). Applying backoff...")
                        self._wait_for_rate_limit(response, rate_limit_attempt)
                        rate_limit_attempt += 1
                        continue
                    if response.status_code == 403:
                        self._log_member_list_permission_error(response)
                        break
                    if response.status_code == 401:
                        self.log("[Scraper] Unauthorized token (HTTP 401).")
                        break
                    if response.status_code == 404:
                        self.log("[Scraper] Guild not found (HTTP 404).")
                        break
                    if response.status_code != 200:
                        self.log(f"[Scraper] Error: {response.status_code}")
                        break

                    members = response.json()
                    rate_limit_attempt = 0
                    if not members:
                        break

                    for member in members:
                        user = member.get("user") or {}
                        u_id = user.get("id")
                        if not u_id:
                            continue
                        if user.get("bot"):
                            continue
                        if u_id != self_id:
                            unique_ids.add(u_id)
                        last_member_id = u_id

                    self.log(f"[Scraper] Found unique: {len(unique_ids)}...")
                    self._wait_for_bucket_reset(response, "member list")
                    self._sleep_with_stop(1)

            if unique_ids:
                self.db.add_targets(list(unique_ids), "discord")
                added_any = True
                self.log(f"[Scraper] Success. Added {len(unique_ids)} new targets to the database.")
        except Exception as e:
            self.log(f"[Scraper] Critical error: {str(e)}")

        self.is_scraping = False
        if on_complete:
            on_complete(added_any)

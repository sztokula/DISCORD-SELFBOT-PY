import httpx
import time

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
        rate_limited = response is not None and response.status_code == 429
        self.metrics.record_request(duration, rate_limited=rate_limited)

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
        self.log(f"[Scraper] Limit endpointu ({reason}) wyczerpany. Czekam {reset_value:.2f}s...")
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
            "[Scraper] Brak uprawnień do pobrania listy członków (HTTP 403). "
            "Sprawdź czy token ma dostęp do serwera i odpowiednie uprawnienia." + details
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
            self.log(f"[Scraper] Nie udało się pobrać @me: {response.status_code}")
            return None
        try:
            data = response.json()
        except Exception:
            self.log("[Scraper] Nie udało się odczytać odpowiedzi @me.")
            return None
        return data.get("id")

    def scrape_history(self, token, channel_id, limit=1000, on_complete=None):
        """Pobiera ID użytkowników, którzy pisali na danym kanale."""
        self.is_scraping = True
        added_any = False
        self.log(f"[Scraper] Rozpoczynanie pobierania z kanału {channel_id}...")
        
        headers = {"Authorization": token}
        url = f"https://discord.com/api/v9/channels/{channel_id}/messages"
        
        unique_ids = set()
        last_msg_id = None
        rate_limit_attempt = 0
        
        try:
            with httpx.Client(headers=headers, timeout=httpx.Timeout(10.0)) as client:
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
                            self.log("[Scraper] Rate limit przekroczony. Kończę.")
                            break
                        self.log("[Scraper] Rate limit! Stosuję backoff...")
                        self._wait_for_rate_limit(response, rate_limit_attempt)
                        rate_limit_attempt += 1
                        continue
                    if response.status_code != 200:
                        self.log(f"[Scraper] Błąd: {response.status_code}")
                        break

                    messages = response.json()
                    rate_limit_attempt = 0
                    if not messages:
                        break
                    
                    for msg in messages:
                        u_id = msg['author']['id']
                        # Nie dodajemy botów ani własnego konta
                        if not msg['author'].get('bot') and u_id != self_id:
                            unique_ids.add(u_id)
                        last_msg_id = msg['id']
                    
                    self.log(f"[Scraper] Znaleziono unikalnych: {len(unique_ids)}...")
                    self._sleep_with_stop(1) # Delay, żeby nie dostać Rate Limit
            
            # Zapis do bazy
            if unique_ids:
                self.db.add_targets(list(unique_ids), "discord")
                added_any = True
                self.log(f"[Scraper] Sukces! Dodano {len(unique_ids)} nowych celów do bazy.")
            
        except Exception as e:
            self.log(f"[Scraper] Krytyczny błąd: {str(e)}")
        
        self.is_scraping = False
        if on_complete:
            on_complete(added_any)

    def stop(self):
        self.is_scraping = False

    def scrape_guild_members(self, token, guild_id, limit=1000, on_complete=None):
        """Pobiera listę członków serwera przez /guilds/{id}/members."""
        self.is_scraping = True
        added_any = False
        self.log(f"[Scraper] Rozpoczynanie pobierania member listy z guild {guild_id}...")

        headers = {"Authorization": token}
        url = f"https://discord.com/api/v9/guilds/{guild_id}/members"

        unique_ids = set()
        last_member_id = None
        rate_limit_attempt = 0

        try:
            with httpx.Client(headers=headers, timeout=httpx.Timeout(10.0)) as client:
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
                            self.log("[Scraper] Rate limit przekroczony. Kończę.")
                            break
                        scope = response.headers.get("X-RateLimit-Scope", "route")
                        scope_info = "global" if response.headers.get("X-RateLimit-Global") else scope
                        self.log(f"[Scraper] Rate limit dla member listy ({scope_info}). Stosuję backoff...")
                        self._wait_for_rate_limit(response, rate_limit_attempt)
                        rate_limit_attempt += 1
                        continue
                    if response.status_code == 403:
                        self._log_member_list_permission_error(response)
                        break
                    if response.status_code == 401:
                        self.log("[Scraper] Token nieautoryzowany (HTTP 401).")
                        break
                    if response.status_code == 404:
                        self.log("[Scraper] Nie znaleziono guildy (HTTP 404).")
                        break
                    if response.status_code != 200:
                        self.log(f"[Scraper] Błąd: {response.status_code}")
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

                    self.log(f"[Scraper] Znaleziono unikalnych: {len(unique_ids)}...")
                    self._wait_for_bucket_reset(response, "member listy")
                    self._sleep_with_stop(1)

            if unique_ids:
                self.db.add_targets(list(unique_ids), "discord")
                added_any = True
                self.log(f"[Scraper] Sukces! Dodano {len(unique_ids)} nowych celów do bazy.")
        except Exception as e:
            self.log(f"[Scraper] Krytyczny błąd: {str(e)}")

        self.is_scraping = False
        if on_complete:
            on_complete(added_any)

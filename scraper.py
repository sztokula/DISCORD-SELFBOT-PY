import httpx
import time

class DiscordScraper:
    def __init__(self, db_manager, log_callback):
        self.db = db_manager
        self.log = log_callback
        self.is_scraping = False
        self.max_retries = 3
        self.backoff_factor = 1.5

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
        time.sleep(wait_time)

    def _fetch_self_id(self, client):
        response = client.get("https://discord.com/api/v9/users/@me")
        if response.status_code != 200:
            self.log(f"[Scraper] Nie udało się pobrać @me: {response.status_code}")
            return None
        try:
            data = response.json()
        except Exception:
            self.log("[Scraper] Nie udało się odczytać odpowiedzi @me.")
            return None
        return data.get("id")

    def scrape_history(self, token, channel_id, limit=1000):
        """Pobiera ID użytkowników, którzy pisali na danym kanale."""
        self.is_scraping = True
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
                    
                    response = client.get(url, params=params)
                    
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
                    time.sleep(1) # Delay, żeby nie dostać Rate Limit
            
            # Zapis do bazy
            if unique_ids:
                self.db.add_targets(list(unique_ids), "discord")
                self.log(f"[Scraper] Sukces! Dodano {len(unique_ids)} nowych celów do bazy.")
            
        except Exception as e:
            self.log(f"[Scraper] Krytyczny błąd: {str(e)}")
        
        self.is_scraping = False

    def stop(self):
        self.is_scraping = False

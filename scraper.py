import httpx
import time

class DiscordScraper:
    def __init__(self, db_manager, log_callback):
        self.db = db_manager
        self.log = log_callback
        self.is_scraping = False

    def scrape_history(self, token, channel_id, limit=1000):
        """Pobiera ID użytkowników, którzy pisali na danym kanale."""
        self.is_scraping = True
        self.log(f"[Scraper] Rozpoczynanie pobierania z kanału {channel_id}...")
        
        headers = {"Authorization": token}
        url = f"https://discord.com/api/v9/channels/{channel_id}/messages"
        
        unique_ids = set()
        last_msg_id = None
        
        try:
            with httpx.Client(headers=headers, timeout=httpx.Timeout(10.0)) as client:
                while len(unique_ids) < limit and self.is_scraping:
                    params = {"limit": 100}
                    if last_msg_id:
                        params["before"] = last_msg_id
                    
                    response = client.get(url, params=params)
                    
                    if response.status_code == 200:
                        messages = response.json()
                        if not messages:
                            break
                        
                        for msg in messages:
                            u_id = msg['author']['id']
                            # Nie dodajemy botów ani samych siebie
                            if not msg['author'].get('bot'):
                                unique_ids.add(u_id)
                            last_msg_id = msg['id']
                        
                        self.log(f"[Scraper] Znaleziono unikalnych: {len(unique_ids)}...")
                        time.sleep(1) # Delay, żeby nie dostać Rate Limit
                    elif response.status_code == 429:
                        self.log("[Scraper] Rate limit! Czekam 5 sekund...")
                        time.sleep(5)
                    else:
                        self.log(f"[Scraper] Błąd: {response.status_code}")
                        break
            
            # Zapis do bazy
            if unique_ids:
                self.db.add_targets(list(unique_ids), "discord")
                self.log(f"[Scraper] Sukces! Dodano {len(unique_ids)} nowych celów do bazy.")
            
        except Exception as e:
            self.log(f"[Scraper] Krytyczny błąd: {str(e)}")
        
        self.is_scraping = False

    def stop(self):
        self.is_scraping = False

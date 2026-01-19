import httpx
import time
import random
import re

class DiscordWorker:
    def __init__(self, db_manager, log_callback):
        self.db = db_manager
        self.log = log_callback
        self.is_running = False

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
        except:
            return False

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
                    time.sleep(random.randint(2, 5))

                # Otwarcie kanału DM
                url_channel = "https://discord.com/api/v9/users/@me/channels"
                response = client.post(url_channel, json={"recipient_id": user_id})
                
                if response.status_code == 200:
                    channel_id = response.json()['id']
                    msg_url = f"https://discord.com/api/v9/channels/{channel_id}/messages"
                    
                    # Losowanie wiadomości (Spintax)
                    final_msg = self.parse_spintax(message)
                    
                    msg_resp = client.post(msg_url, json={"content": final_msg})
                    return msg_resp.status_code == 200, "Success" if msg_resp.status_code == 200 else f"Code: {msg_resp.status_code}"
                return False, f"Channel Error: {response.status_code}"
            except Exception as e:
                return False, str(e)

    def run_mission(self, message, delay_min, delay_max, use_friend_req=False):
        self.is_running = True
        self.log("[Mission] Startujemy...")
        
        while self.is_running:
            accounts = self.db.get_active_accounts("discord")
            if not accounts: break

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
                success, msg = self.send_dm(token, u_id, message, proxy, use_friend_req)
                
                if success:
                    self.db.update_target_status(t_id, "Sent")
                    self.db.increment_sent_counter(acc_id)
                    self.log(f"[OK] DM wysłany do {u_id}")
                else:
                    self.db.update_target_status(t_id, "Failed", msg)
                    self.log(f"[!] Błąd {u_id}: {msg}")

                time.sleep(random.randint(delay_min, delay_max))

    def stop(self):
        self.is_running = False

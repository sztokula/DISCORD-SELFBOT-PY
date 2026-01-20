import httpx
import time
import random

class DiscordJoiner:
    def __init__(self, db_manager, log_callback, captcha_solver=None):
        self.db = db_manager
        self.log = log_callback
        self.is_running = False
        self.captcha_solver = captcha_solver

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
            self.log(f"[Auth] Nie udało się odświeżyć tokenu dla konta {account_id}: {e}")
            return None
        if fresh_token and fresh_token != current_token:
            self.log(f"[Auth] Odświeżono token dla konta {account_id}.")
            return fresh_token
        return None

    def _handle_unauthorized(self, client, account_id, current_token, refreshed):
        if refreshed:
            self.log(f"[Joiner] Token dla konta {account_id} nadal niepoprawny. Dezaktywuję konto.")
            if account_id is not None:
                self.db.update_account_status(account_id, "Banned/Dead")
                self.db.remove_account(account_id)
            return None, True
        new_token = self._refresh_token(account_id, current_token)
        if new_token:
            client.headers["Authorization"] = new_token
            return new_token, True
        self.log(f"[Joiner] Brak nowego tokenu dla konta {account_id}. Dezaktywuję konto.")
        if account_id is not None:
            self.db.update_account_status(account_id, "Banned/Dead")
            self.db.remove_account(account_id)
        return None, True

    def join_server(self, account_id, token, invite_code, proxy=None):
        """invite_code: tylko końcówka linku, np. 'fajny-serwer' z discord.gg/fajny-serwer"""
        # Czyścimy kod zaproszenia na wypadek gdyby ktoś wkleił cały link
        invite_code = invite_code.split("/")[-1]
        
        url = f"https://discord.com/api/v9/invites/{invite_code}"
        headers = {
            "Authorization": token,
            "Content-Type": "application/json",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        }
        
        proxies = {"all://": proxy} if proxy else None
        
        max_retries = 3
        backoff_factor = 1.5

        try:
            with httpx.Client(proxies=proxies, headers=headers, timeout=httpx.Timeout(10.0)) as client:
                refreshed = False
                for attempt in range(max_retries + 1):
                    response = client.post(url, json={})
                    if response.status_code == 401:
                        token, refreshed = self._handle_unauthorized(client, account_id, token, refreshed)
                        if token:
                            continue
                        return False, "Unauthorized (token)"
                    if response.status_code == 200:
                        return True, "Sukces"
                    if response.status_code in {400, 403}:
                        captcha_info = self._extract_captcha(response)
                        if captcha_info and self.captcha_solver:
                            solved, token_or_err = self.captcha_solver.solve_captcha(captcha_info)
                            if solved:
                                payload = {"captcha_key": token_or_err}
                                if captcha_info.get("rqtoken"):
                                    payload["captcha_rqtoken"] = captcha_info["rqtoken"]
                                retry_resp = client.post(url, json=payload)
                                if retry_resp.status_code == 200:
                                    return True, "Sukces (captcha)"
                                return False, f"Błąd po captcha: {retry_resp.status_code}"
                            return False, f"Captcha error: {token_or_err}"
                        return False, "Wymagana weryfikacja (Captcha/Telefon)"
                    if response.status_code == 429:
                        retry_after = self._get_retry_after(response)
                        wait_time = retry_after * (backoff_factor ** attempt)
                        self._sleep_with_stop(wait_time)
                        continue
                    return False, f"Błąd {response.status_code}"
        except Exception as e:
            return False, str(e)

        return False, "Rate Limit (po ponownych próbach)"

    def run_mass_join(self, invite_codes, delay_min, delay_max):
        self.is_running = True
        self.db.reset_daily_counters()
        accounts = self.db.get_active_accounts("discord")
        
        if not accounts:
            self.log("[Joiner] Brak aktywnych kont do operacji.")
            return

        if not invite_codes:
            self.log("[Joiner] Brak poprawnych zaproszeń.")
            self.is_running = False
            return

        self.log(f"[Joiner] Rozpoczynam dołączanie {len(accounts)} kont do {len(invite_codes)} zaproszeń (losowo na konto).")

        did_join_attempt = False
        for acc in accounts:
            if not self.is_running: break
            
            acc_id, _, token, proxy, _, _, _, _, join_limit, join_today, _ = acc
            if join_today >= join_limit:
                self.log(f"[Joiner] Konto {acc_id}: dzienny limit joinów osiągnięty ({join_today}/{join_limit}).")
                continue
            invite_code = random.choice(invite_codes)
            success, msg = self.join_server(acc_id, token, invite_code, proxy)
            
            if success:
                self.db.increment_join_counter(acc_id)
                self.log(f"[Joiner] Konto {acc_id}: DOŁĄCZONO ({invite_code}).")
            else:
                self.log(f"[Joiner] Konto {acc_id}: BŁĄD ({msg}) [{invite_code}]")
            
            did_join_attempt = True
            # BARDZO WAŻNE: Duży odstęp czasu przy dołączaniu
            wait = random.randint(delay_min, delay_max)
            self.log(f"[Joiner] Oczekiwanie {wait}s przed następnym kontem...")
            self._sleep_with_stop(wait)

        if self.is_running and not did_join_attempt:
            self.log("[Joiner] Wszystkie konta mają dzienny limit joinów.")
        self.log("[Joiner] Proces masowego dołączania zakończony.")
        self.is_running = False

    def stop(self):
        self.is_running = False

    def _sleep_with_stop(self, total_seconds, interval=0.5):
        end_time = time.monotonic() + max(0.0, total_seconds)
        while self.is_running and time.monotonic() < end_time:
            remaining = end_time - time.monotonic()
            time.sleep(min(interval, max(0.0, remaining)))

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

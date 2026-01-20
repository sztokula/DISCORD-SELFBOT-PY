import httpx

class TokenManager:
    def __init__(self, db_manager, log_callback):
        self.db = db_manager
        self.log = log_callback

    def validate_token(self, token):
        """Sprawdza czy token jest żywy i pobiera nazwę użytkownika."""
        url = "https://discord.com/api/v9/users/@me"
        headers = {"Authorization": token}
        
        try:
            with httpx.Client(headers=headers, timeout=httpx.Timeout(10.0)) as client:
                response = client.get(url)
                if response.status_code == 200:
                    data = response.json()
                    return True, f"{data['username']}#{data['discriminator']}"
                elif response.status_code == 401:
                    return False, "Invalid Token"
                else:
                    return False, f"Error {response.status_code}"
        except Exception as e:
            return False, str(e)

    def check_all_accounts(self):
        self.log("[Checker] Rozpoczynam weryfikację bazy tokenów...")
        accounts = self.db.get_active_accounts("discord")
        
        valid_count = 0
        for acc in accounts:
            acc_id, _, token, _, _, _, _, _, _, _, _ = acc
            is_valid, info = self.validate_token(token)
            
            if is_valid:
                self.log(f"[OK] Konto {acc_id}: {info}")
                valid_count += 1
            else:
                self.log(f"[DEAD] Konto {acc_id}: {info}. Dezaktywuję w bazie.")
                self.db.update_account_status(acc_id, "Banned/Dead")
                self.db.remove_account(acc_id)
        
        self.log(f"[Checker] Zakończono. Aktywne: {valid_count}/{len(accounts)}")

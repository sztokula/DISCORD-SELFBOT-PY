import base64
import time
from pathlib import Path

import httpx


class ProfileUpdater:
    def __init__(self, db_manager, log_callback, metrics=None):
        self.db = db_manager
        self.log = log_callback
        self.metrics = metrics
        self.max_retries = 3
        self.backoff_factor = 1.5

    def _record_request(self, duration, response=None):
        if not self.metrics:
            return
        status_code = response.status_code if response is not None else None
        rate_limited = status_code == 429
        self.metrics.record_request(duration, status_code=status_code, rate_limited=rate_limited)

    def _wait_for_rate_limit(self, response, attempt):
        retry_after = response.headers.get("Retry-After")
        wait_time = 5.0
        if retry_after:
            try:
                wait_time = float(retry_after)
            except ValueError:
                pass
        wait_time *= self.backoff_factor ** attempt
        time.sleep(min(wait_time, 60))

    def load_avatar_data(self, avatar_path):
        if not avatar_path:
            return None, "Avatar path is empty."
        path = Path(avatar_path)
        if not path.exists():
            return None, f"Avatar file not found: {avatar_path}"
        ext = path.suffix.lower()
        mime_map = {
            ".png": "image/png",
            ".jpg": "image/jpeg",
            ".jpeg": "image/jpeg",
            ".gif": "image/gif",
            ".webp": "image/webp",
        }
        mime = mime_map.get(ext)
        if not mime:
            return None, "Unsupported avatar file type."
        try:
            data = path.read_bytes()
        except OSError as exc:
            return None, f"Failed to read avatar file: {exc}"
        encoded = base64.b64encode(data).decode("ascii")
        return f"data:{mime};base64,{encoded}", None

    def update_profiles(
        self,
        base_name,
        avatar_data,
        change_name=True,
        change_avatar=True,
        append_suffix=True,
        allowed_account_ids=None,
    ):
        accounts = self.db.get_active_accounts("discord")
        if allowed_account_ids:
            allowed_set = set(allowed_account_ids)
            accounts = [acc for acc in accounts if acc[0] in allowed_set]
        if not accounts:
            self.log("[Profile] No active accounts.")
            return
        if not change_name and not change_avatar:
            self.log("[Profile] Nothing to update (name/avatar disabled).")
            return

        for index, acc in enumerate(accounts, start=1):
            acc_id, _, token, proxy, _, _, _, _, _, _, _ = acc
            payload = {}
            if change_name:
                suffix = str(index) if append_suffix else ""
                payload["username"] = f"{base_name}{suffix}"
            if change_avatar:
                payload["avatar"] = avatar_data
            if not payload:
                self.log("[Profile] Empty profile payload.")
                return

            headers = {
                "Authorization": token,
                "Content-Type": "application/json",
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            }
            proxies = {"all://": proxy} if proxy else None
            with httpx.Client(proxies=proxies, headers=headers, timeout=httpx.Timeout(10.0)) as client:
                updated = False
                for attempt in range(self.max_retries + 1):
                    start = time.monotonic()
                    response = client.patch("https://discord.com/api/v9/users/@me", json=payload)
                    self._record_request(time.monotonic() - start, response)
                    if response.status_code == 200:
                        self.log(f"[Profile] Account {acc_id}: profile updated.")
                        updated = True
                        break
                    if response.status_code == 401:
                        self.log(f"[Profile] Account {acc_id}: unauthorized, removing.")
                        self.db.update_account_status(acc_id, "Banned/Dead")
                        self.db.remove_account(acc_id)
                        updated = True
                        break
                    if response.status_code == 429:
                        self._wait_for_rate_limit(response, attempt)
                        continue
                    self.log(f"[Profile] Account {acc_id}: update failed ({response.status_code}).")
                    break
                if not updated:
                    self.log(f"[Profile] Account {acc_id}: update failed after retries.")
            time.sleep(1.0)

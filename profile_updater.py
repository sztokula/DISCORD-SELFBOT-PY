import base64
import hashlib
import random
import time
from datetime import datetime
from pathlib import Path

import httpx
from proxy_utils import httpx_client
from super_properties import set_super_properties_header
from client_identity import USER_AGENT
from behavior_version import CURRENT_BEHAVIOR_VERSION, get_behavior_version, seeded_rng


class ProfileUpdater:
    def __init__(self, db_manager, log_callback, metrics=None, telemetry=None):
        self.db = db_manager
        self.log = log_callback
        self.metrics = metrics
        self.telemetry = telemetry
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

    def _get_setting_float(self, key, default, min_value=None, max_value=None):
        try:
            raw = self.db.get_setting(key, "")
        except Exception:
            raw = ""
        if raw in (None, ""):
            value = default
        else:
            try:
                value = float(raw)
            except (TypeError, ValueError):
                value = default
        if min_value is not None:
            value = max(min_value, value)
        if max_value is not None:
            value = min(max_value, value)
        return value

    def _parse_ts(self, value):
        if not value:
            return None
        try:
            return datetime.fromisoformat(str(value))
        except Exception:
            return None

    def _seconds_since(self, value):
        ts = self._parse_ts(value)
        if not ts:
            return None
        return max(0.0, (datetime.now() - ts).total_seconds())

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

        global_min_interval = self._get_setting_float("profile_min_interval_seconds", 3600.0, 0.0, None)
        name_min_interval = self._get_setting_float("profile_name_min_interval_seconds", 21600.0, 0.0, None)
        avatar_min_interval = self._get_setting_float("profile_avatar_min_interval_seconds", 86400.0, 0.0, None)
        step_delay_min = self._get_setting_float("profile_step_delay_min_seconds", 1.0, 0.0, None)
        step_delay_max = self._get_setting_float("profile_step_delay_max_seconds", 3.0, step_delay_min, None)

        for index, acc in enumerate(accounts, start=1):
            acc_id, _, token, proxy, _, _, _, _, _, _, _ = acc
            history = self.db.get_profile_history(acc_id) or {}
            version = get_behavior_version(self.db, token, CURRENT_BEHAVIOR_VERSION)
            base_rng = seeded_rng(token or "anon", version, "profile_update")
            recent_seconds = self._seconds_since(history.get("updated_at"))
            if global_min_interval and recent_seconds is not None and recent_seconds < global_min_interval:
                self.log(
                    f"[Profile] Account {acc_id}: skipped (recent profile change {int(recent_seconds)}s ago)."
                )
                continue

            candidates = []
            desired_name = None
            avatar_hash = None
            if change_name:
                suffix = str(index) if append_suffix else ""
                desired_name = f"{base_name}{suffix}"
                if desired_name:
                    last_name = history.get("last_username")
                    if desired_name != last_name:
                        elapsed = self._seconds_since(history.get("name_updated_at"))
                        if elapsed is None or elapsed >= name_min_interval:
                            candidates.append(("username", desired_name, elapsed))
            if change_avatar and avatar_data:
                avatar_hash = hashlib.sha256(avatar_data.encode("utf-8")).hexdigest()
                last_hash = history.get("last_avatar_hash")
                if avatar_hash and avatar_hash != last_hash:
                    elapsed = self._seconds_since(history.get("avatar_updated_at"))
                    if elapsed is None or elapsed >= avatar_min_interval:
                        candidates.append(("avatar", avatar_data, elapsed))

            if not candidates:
                self.log(f"[Profile] Account {acc_id}: no eligible profile changes.")
                continue

            if len(candidates) > 1:
                def _elapsed_value(item):
                    elapsed_val = item[2]
                    return elapsed_val if elapsed_val is not None else 1e9
                candidates.sort(key=_elapsed_value, reverse=True)
                top = candidates[0]
                if len(candidates) > 1 and _elapsed_value(candidates[0]) == _elapsed_value(candidates[1]):
                    top = base_rng.choice(candidates[:2])
                chosen = top
            else:
                chosen = candidates[0]

            payload = {}
            changed_name = False
            changed_avatar = False
            if chosen[0] == "username":
                payload["username"] = chosen[1]
                changed_name = True
            elif chosen[0] == "avatar":
                payload["avatar"] = chosen[1]
                changed_avatar = True

            if not payload:
                self.log("[Profile] Empty profile payload.")
                continue

            user_agent = USER_AGENT
            headers = {
                "Authorization": token,
                "Content-Type": "application/json",
                "User-Agent": user_agent,
            }
            set_super_properties_header(headers, self.db)
            with httpx_client(
                proxy,
                headers=headers,
                timeout=httpx.Timeout(10.0),
                cookie_db=self.db,
                cookie_token=lambda: token,
            ) as client:
                updated = False
                for attempt in range(self.max_retries + 1):
                    start = time.monotonic()
                    response = client.patch("https://discord.com/api/v9/users/@me", json=payload)
                    self._record_request(time.monotonic() - start, response)
                    if response.status_code == 200:
                        now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                        try:
                            self.db.update_profile_history(
                                acc_id,
                                username=payload.get("username") if changed_name else None,
                                avatar_hash=avatar_hash if changed_avatar else None,
                                name_updated_at=now_str if changed_name else None,
                                avatar_updated_at=now_str if changed_avatar else None,
                                updated_at=now_str,
                            )
                        except Exception:
                            pass
                        self.log(f"[Profile] Account {acc_id}: profile updated.")
                        self.log(f"[Info] Profile updated for account {acc_id}.")
                        if self.telemetry:
                            self.telemetry.send_science(
                                token,
                                user_agent,
                                "profile_update",
                                properties={
                                    "change_name": bool(changed_name),
                                    "change_avatar": bool(changed_avatar),
                                },
                                proxy=proxy,
                            )
                        updated = True
                        break
                    if response.status_code == 401:
                        try:
                            self.db.clear_token_cookies(token)
                        except Exception:
                            pass
                        self.log(f"[Profile] Account {acc_id}: unauthorized, removing.")
                        self.db.update_account_status(acc_id, "Banned/Dead")
                        self.db.remove_account(acc_id)
                        updated = True
                        break
                    if response.status_code == 403:
                        try:
                            self.db.clear_token_cookies(token)
                        except Exception:
                            pass
                        self.log(f"[Profile] Account {acc_id}: forbidden (403).")
                        updated = True
                        break
                    if response.status_code == 429:
                        self._wait_for_rate_limit(response, attempt)
                        continue
                    self.log(f"[Profile] Account {acc_id}: update failed ({response.status_code}).")
                    break
                if not updated:
                    self.log(f"[Profile] Account {acc_id}: update failed after retries.")
            scale = base_rng.uniform(0.8, 1.2)
            adj_min = max(0.0, step_delay_min * scale)
            adj_max = max(adj_min, step_delay_max * scale)
            delay = random.uniform(adj_min, adj_max) if adj_max > adj_min else adj_min
            time.sleep(max(0.0, delay))

import re
import threading
import time
from datetime import datetime, timedelta
from urllib.parse import urljoin

from proxy_utils import httpx_client

LOGIN_URL = "https://discord.com/login"
DEFAULT_USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    "Chrome/120.0.0.0 Safari/537.36"
)

BUILD_NUMBER_PATTERNS = [
    re.compile(r'build_number"\s*:\s*"(\d+)"'),
    re.compile(r"build_number\s*:\s*(\d+)"),
    re.compile(r'build_number\s*:\s*"(\d+)"'),
    re.compile(r'client_build_number"\s*:\s*"(\d+)"'),
    re.compile(r"client_build_number\s*:\s*(\d+)"),
    re.compile(r'client_build_number\s*:\s*"(\d+)"'),
]


class BuildNumberUpdater:
    def __init__(self, db_manager, log_callback=None, interval_seconds=86400):
        self.db = db_manager
        self.log = log_callback
        self.interval_seconds = interval_seconds
        self._stop = threading.Event()

    def stop(self):
        self._stop.set()

    def run_forever(self):
        while not self._stop.is_set():
            try:
                self.run_once()
            except Exception as exc:
                self._log(f"[Build] Update failed: {exc}")
            self._sleep_interval()

    def run_once(self, force=False):
        if not force and not self._should_check():
            return False

        headers = {"User-Agent": DEFAULT_USER_AGENT}
        js_urls = []
        build_number = None
        source_url = None
        checked_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        with httpx_client(None, headers=headers, timeout=15.0) as client:
            response = client.get(LOGIN_URL)
            if response.status_code != 200:
                self._log(f"[Build] Login fetch failed: {response.status_code}")
                self._set_checked_at(checked_at)
                return False
            html = response.text
            js_urls = self._extract_js_urls(html, LOGIN_URL)
            if not js_urls:
                self._log("[Build] No JS assets found on login page.")
                self._set_checked_at(checked_at)
                return False

            for js_url in js_urls:
                try:
                    js_resp = client.get(js_url)
                except Exception:
                    continue
                if js_resp.status_code != 200:
                    continue
                build_number = self._extract_build_number(js_resp.text)
                if build_number:
                    source_url = js_url
                    break

        self._set_checked_at(checked_at)
        if not build_number:
            self._log("[Build] build_number not found in JS assets.")
            return False

        self.db.set_setting("client_build_number", str(build_number))
        self.db.set_setting("client_build_number_updated_at", checked_at)
        if source_url:
            self.db.set_setting("client_build_number_js_url", source_url)
        self._log(f"[Build] Updated build_number to {build_number}.")
        return True

    def _should_check(self):
        raw = self.db.get_setting("client_build_number_checked_at", "")
        if not raw:
            return True
        try:
            last = datetime.fromisoformat(raw)
        except (TypeError, ValueError):
            return True
        return datetime.now() - last >= timedelta(seconds=self.interval_seconds)

    def _set_checked_at(self, timestamp):
        self.db.set_setting("client_build_number_checked_at", timestamp)

    def _extract_js_urls(self, html, base_url):
        matches = re.findall(r'<script[^>]+src="([^"]+\.js[^"]*)"', html, re.IGNORECASE)
        urls = []
        for src in matches:
            full = urljoin(base_url, src)
            urls.append(full)
        urls = self._prioritize_js_urls(urls)
        return urls

    def _prioritize_js_urls(self, urls):
        seen = set()
        unique = []
        for url in urls:
            if url in seen:
                continue
            seen.add(url)
            unique.append(url)
        scored = []
        for url in unique:
            score = 0
            lowered = url.lower()
            if "app" in lowered or "main" in lowered:
                score += 2
            if "web" in lowered:
                score += 1
            scored.append((score, url))
        scored.sort(key=lambda item: item[0], reverse=True)
        return [url for _, url in scored]

    def _extract_build_number(self, text):
        for pattern in BUILD_NUMBER_PATTERNS:
            match = pattern.search(text)
            if match:
                return match.group(1)
        return None

    def _sleep_interval(self):
        remaining = self.interval_seconds
        while remaining > 0 and not self._stop.is_set():
            chunk = min(60, remaining)
            time.sleep(chunk)
            remaining -= chunk

    def _log(self, message):
        if self.log:
            self.log(message)


if __name__ == "__main__":
    from database import DatabaseManager

    db = DatabaseManager()
    updater = BuildNumberUpdater(db)
    updater.run_forever()

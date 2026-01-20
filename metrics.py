import threading
import time


class HealthMetrics:
    def __init__(self):
        self._lock = threading.Lock()
        self._start_time = time.monotonic()
        self._total_requests = 0
        self._total_duration = 0.0
        self._rate_limit_count = 0
        self._last_rate_limit_at = None
        self._rate_limit_events = []
        self._server_error_events = []
        self._window_seconds = 60.0

    def _prune_events(self, now):
        cutoff = now - self._window_seconds
        if self._rate_limit_events:
            self._rate_limit_events = [ts for ts in self._rate_limit_events if ts >= cutoff]
        if self._server_error_events:
            self._server_error_events = [ts for ts in self._server_error_events if ts >= cutoff]

    def record_request(self, duration_seconds, status_code=None, rate_limited=False):
        with self._lock:
            self._total_requests += 1
            self._total_duration += max(0.0, float(duration_seconds))
            now = time.monotonic()
            if rate_limited or status_code == 429:
                self._rate_limit_count += 1
                self._last_rate_limit_at = now
                self._rate_limit_events.append(now)
            if status_code is not None and 500 <= int(status_code) <= 599:
                self._server_error_events.append(now)
            self._prune_events(now)

    def snapshot(self):
        with self._lock:
            uptime = time.monotonic() - self._start_time
            total_requests = self._total_requests
            avg_ms = 0.0
            if total_requests > 0:
                avg_ms = (self._total_duration / total_requests) * 1000.0
            rate_count = self._rate_limit_count
            last_age = None
            if self._last_rate_limit_at is not None:
                last_age = max(0.0, time.monotonic() - self._last_rate_limit_at)
            now = time.monotonic()
            self._prune_events(now)
            recent_rate = len(self._rate_limit_events)
            recent_server = len(self._server_error_events)
        return {
            "uptime_seconds": uptime,
            "total_requests": total_requests,
            "avg_request_ms": avg_ms,
            "rate_limit_count": rate_count,
            "last_rate_limit_age_seconds": last_age,
            "recent_rate_limit_count": recent_rate,
            "recent_server_error_count": recent_server,
            "alert_window_seconds": self._window_seconds,
        }

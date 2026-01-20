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

    def record_request(self, duration_seconds, rate_limited=False):
        with self._lock:
            self._total_requests += 1
            self._total_duration += max(0.0, float(duration_seconds))
            if rate_limited:
                self._rate_limit_count += 1
                self._last_rate_limit_at = time.monotonic()

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
        return {
            "uptime_seconds": uptime,
            "total_requests": total_requests,
            "avg_request_ms": avg_ms,
            "rate_limit_count": rate_count,
            "last_rate_limit_age_seconds": last_age,
        }

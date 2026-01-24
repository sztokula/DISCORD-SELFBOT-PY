import random
import threading
import time


def gaussian_delay(min_seconds, max_seconds, rng=None):
    try:
        min_val = float(min_seconds)
        max_val = float(max_seconds)
    except (TypeError, ValueError):
        return 0.0
    if max_val < min_val:
        min_val, max_val = max_val, min_val
    if max_val == min_val:
        return max_val
    mean = (min_val + max_val) / 2.0
    sigma = (max_val - min_val) / 6.0
    if sigma <= 0:
        return mean
    rand = rng or random
    value = rand.gauss(mean, sigma)
    if value < min_val:
        return min_val
    if value > max_val:
        return max_val
    return value


class DelayController:
    def __init__(self):
        self._lock = threading.Lock()
        self._account_state = {}
        self._global_next_allowed = 0.0

    def _get_state(self, account_id, rng=None):
        state = self._account_state.get(account_id)
        if state:
            return state
        rand = rng or random
        state = {
            "dm_since_pause": 0,
            "dm_since_idle": 0,
            "pause_every": rand.randint(2, 5),
            "idle_every": rand.randint(4, 9),
            "last_send_ts": 0.0,
        }
        self._account_state[account_id] = state
        return state

    def _age_factor(self, age_hours):
        if age_hours is None:
            return 1.2
        if age_hours < 24:
            return 2.2
        if age_hours < 72:
            return 1.6
        if age_hours < 168:
            return 1.3
        if age_hours < 720:
            return 1.1
        return 1.0

    def _sent_factor(self, sent_today):
        try:
            sent = max(0, int(sent_today))
        except (TypeError, ValueError):
            sent = 0
        return 1.0 + min(sent, 30) / 60.0

    def _diurnal_pause(self, rng=None):
        rand = rng or random
        hour = time.localtime().tm_hour
        if 0 <= hour < 6:
            return rand.uniform(120.0, 600.0)
        if 6 <= hour < 9:
            return rand.uniform(10.0, 45.0)
        if 22 <= hour <= 23:
            return rand.uniform(20.0, 90.0)
        return 0.0

    def next_delay(
        self,
        *,
        account_id,
        base_min,
        base_max,
        account_age_hours=None,
        sent_today=0,
        did_send=True,
        recently_reconnected=False,
        rng=None,
    ):
        rand = rng or random
        base = gaussian_delay(base_min, base_max, rng=rand)
        if base <= 0:
            base = rand.uniform(1.0, 3.0)

        age_factor = self._age_factor(account_age_hours)
        sent_factor = self._sent_factor(sent_today)
        delay = base * age_factor * sent_factor

        extra = 0.0
        with self._lock:
            state = self._get_state(account_id, rng=rand)
            if recently_reconnected:
                state["dm_since_pause"] = 0
                state["dm_since_idle"] = 0
                state["pause_every"] = rand.randint(2, 5)
                state["idle_every"] = rand.randint(4, 9)
                extra += rand.uniform(15.0, 60.0)

            if did_send:
                state["dm_since_pause"] += 1
                state["dm_since_idle"] += 1
                state["last_send_ts"] = time.monotonic()

            if state["dm_since_pause"] >= state["pause_every"] and did_send:
                extra += rand.uniform(5.0, 25.0)
                state["dm_since_pause"] = 0
                state["pause_every"] = rand.randint(2, 5)

            if state["dm_since_idle"] >= state["idle_every"] and did_send:
                extra += rand.uniform(120.0, 900.0)
                state["dm_since_idle"] = 0
                state["idle_every"] = rand.randint(4, 9)

            extra += self._diurnal_pause(rng=rand)

            now = time.monotonic()
            if now < self._global_next_allowed:
                extra += self._global_next_allowed - now
            self._global_next_allowed = now + delay + extra

        jitter = rand.uniform(0.0, max(1.0, delay * 0.1))
        return max(0.0, delay + extra + jitter)

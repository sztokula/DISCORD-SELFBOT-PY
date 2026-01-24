import hashlib
import random

CURRENT_BEHAVIOR_VERSION = "2026.01.24"


def get_behavior_version(db, token, default_version=CURRENT_BEHAVIOR_VERSION):
    if not token or not db:
        return default_version
    try:
        return db.get_token_behavior_version(token, default_version) or default_version
    except Exception:
        return default_version


def seeded_rng(token, version, namespace="default"):
    seed = hashlib.sha256(f"{token}|{version}|{namespace}".encode("utf-8")).hexdigest()
    return random.Random(int(seed[:16], 16))

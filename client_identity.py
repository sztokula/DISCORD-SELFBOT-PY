import os

MIN_CHROME_VERSION = 110
CHROME_VERSION = max(120, MIN_CHROME_VERSION)
IMPERSONATE_PROFILE = f"chrome{CHROME_VERSION}"
USER_AGENT = (
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
    "AppleWebKit/537.36 (KHTML, like Gecko) "
    f"Chrome/{CHROME_VERSION}.0.0.0 Safari/537.36"
)

JA3_FINGERPRINT = (os.getenv("TLS_JA3", "") or "").strip() or None
AKAMAI_FINGERPRINT = (os.getenv("TLS_AKAMAI", "") or "").strip() or None

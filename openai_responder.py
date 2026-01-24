import random
import re
import httpx
import time
from proxy_utils import httpx_client, load_external_proxy, resolve_proxy_for_traffic
from behavior_version import CURRENT_BEHAVIOR_VERSION, get_behavior_version, seeded_rng


DEFAULT_MODEL = "gpt-4o-mini"
DEFAULT_SYSTEM_PROMPT = (
    "You are a friendly human chatting on Discord. Reply naturally, keep it short, "
    "and avoid sounding salesy or pushy. Do not mention being an AI."
)


class OpenAIResponder:
    def __init__(self, db_manager, log_callback=None):
        self.db = db_manager
        self.log = log_callback
        self._style_cache = {}

    def _log(self, message):
        if self.log:
            self.log(message)

    def _get_api_key(self):
        return (self.db.get_setting("openai_api_key", "") or "").strip()

    def _get_model(self):
        value = (self.db.get_setting("openai_model", "") or "").strip()
        return value or DEFAULT_MODEL

    def _get_system_prompt(self):
        value = (self.db.get_setting("openai_system_prompt", "") or "").strip()
        return value or DEFAULT_SYSTEM_PROMPT

    def is_enabled(self):
        value = self.db.get_setting("auto_reply_enabled", None)
        if value in (None, ""):
            return False
        if isinstance(value, str):
            return value.strip().lower() in {"1", "true", "yes", "on"}
        return bool(value)

    def _mutations_enabled(self):
        value = self.db.get_setting("auto_reply_mutation", None)
        if value in (None, ""):
            return True
        if isinstance(value, str):
            return value.strip().lower() in {"1", "true", "yes", "on"}
        return bool(value)

    def _get_behavior_version(self, token):
        return get_behavior_version(self.db, token, CURRENT_BEHAVIOR_VERSION)

    def _mutation_rng(self, token, version):
        if not token:
            return random.Random(time.time_ns())
        base_rng = seeded_rng(token, version, "ai_mutation")
        seed = time.time_ns() ^ base_rng.getrandbits(64)
        return random.Random(seed)

    def _token_style(self, token, version):
        if not token:
            return {
                "reaction_only_rate": 0.0,
                "emoji_rate": 0.2,
                "typo_rate": 0.0,
                "pause_rate": 0.1,
                "interrupt_rate": 0.1,
                "lowercase_rate": 0.0,
                "shorten_rate": 0.1,
                "cutoff_rate": 0.05,
                "no_reply_rate": 0.03,
                "min_len": 8,
                "max_len": 160,
                "emojis": ["🙂", "👍", "🙌"],
                "reactions": ["👍", "👌", "🙂", "ok", "lol"],
            }
        cache_key = f"{token}:{version}"
        cached = self._style_cache.get(cache_key)
        if cached:
            return cached
        rng = seeded_rng(token, version, "ai_style")
        emoji_pool = ["🙂", "😂", "🙌", "👍", "✨", "🤝", "🙏", "😅", "😎", "👌"]
        reactions = ["👍", "👌", "🙂", "😂", "ok", "lol", "👀", "🙏"]
        style = {
            "reaction_only_rate": rng.uniform(0.02, 0.15),
            "emoji_rate": rng.uniform(0.2, 0.85),
            "typo_rate": rng.uniform(0.02, 0.08),
            "pause_rate": rng.uniform(0.1, 0.5),
            "lowercase_rate": rng.uniform(0.05, 0.5),
            "shorten_rate": rng.uniform(0.15, 0.5),
            "min_len": rng.randint(6, 16),
            "max_len": rng.randint(80, 180),
            "emojis": rng.sample(emoji_pool, k=rng.randint(2, 5)),
            "reactions": rng.sample(reactions, k=rng.randint(3, 6)),
        }
        extras_rng = seeded_rng(token, version, "ai_style_extras")
        style["interrupt_rate"] = style.get("pause_rate", 0.0)
        style["cutoff_rate"] = extras_rng.uniform(0.04, 0.18)
        style["no_reply_rate"] = extras_rng.uniform(0.02, 0.08)
        self._style_cache[cache_key] = style
        return style

    def _truncate_text(self, text, target_len):
        if target_len <= 0 or len(text) <= target_len:
            return text
        cutoff = text.rfind(".", 0, target_len)
        if cutoff == -1:
            cutoff = text.rfind("!", 0, target_len)
        if cutoff == -1:
            cutoff = text.rfind("?", 0, target_len)
        if cutoff == -1:
            cutoff = text.rfind(",", 0, target_len)
        if cutoff == -1:
            cutoff = text.rfind(" ", 0, target_len)
        if cutoff == -1:
            cutoff = target_len
        return text[:cutoff].rstrip()

    def _inject_pause(self, text):
        if "..." in text:
            return text
        for sep in [". ", "! ", "? ", ", "]:
            if sep in text:
                parts = text.split(sep, 1)
                if len(parts) == 2:
                    return f"{parts[0]}... {parts[1]}"
        words = text.split()
        if len(words) > 4:
            mid = len(words) // 2
            return " ".join(words[:mid] + ["..."] + words[mid:])
        return text + "..."

    def _hard_cutoff(self, text, rng):
        if not text:
            return text
        if len(text) < 6:
            return text + "..."
        min_cut = max(2, int(len(text) * 0.35))
        max_cut = max(min_cut + 1, int(len(text) * 0.8))
        cut = rng.randint(min_cut, max_cut)
        snippet = text[:cut].rstrip()
        if not snippet:
            return text
        if snippet.endswith("..."):
            return snippet
        snippet = snippet.rstrip(".,;:!?")
        if not snippet:
            return text
        return snippet + "..."

    def _add_typo(self, text, rng):
        words = re.findall(r"[A-Za-z]{4,}", text)
        if not words:
            return text
        word = rng.choice(words)
        idx = text.find(word)
        if idx < 0:
            return text
        if len(word) < 4:
            return text
        pos = rng.randint(1, len(word) - 2)
        typo = list(word)
        typo[pos - 1], typo[pos] = typo[pos], typo[pos - 1]
        typo_word = "".join(typo)
        return text[:idx] + typo_word + text[idx + len(word):]

    def _maybe_lowercase(self, text):
        if not text:
            return text
        return text[0].lower() + text[1:]

    def _append_emoji(self, text, emojis, rng):
        if any(e in text for e in emojis):
            return text
        return f"{text} {rng.choice(emojis)}"

    def should_skip_reply(self, token):
        if not self._mutations_enabled():
            return None
        version = self._get_behavior_version(token)
        style = self._token_style(token, version)
        rng = self._mutation_rng(token, version)
        if rng.random() < style.get("no_reply_rate", 0.0):
            return "no_reply"
        return None

    def _mutate_reply(self, text, token):
        text = (text or "").strip()
        if not text or not self._mutations_enabled():
            return text
        version = self._get_behavior_version(token)
        style = self._token_style(token, version)
        rng = self._mutation_rng(token, version)
        if len(text) < style["min_len"] and rng.random() < 0.5:
            return text
        if rng.random() < style["reaction_only_rate"]:
            return rng.choice(style["reactions"])
        if rng.random() < style["shorten_rate"] or len(text) > style["max_len"]:
            target = min(style["max_len"], max(style["min_len"], int(len(text) * rng.uniform(0.55, 0.9))))
            text = self._truncate_text(text, target)
        interrupt_rate = style.get("interrupt_rate", style.get("pause_rate", 0.0))
        if rng.random() < interrupt_rate:
            text = self._inject_pause(text)
        if rng.random() < style["typo_rate"]:
            text = self._add_typo(text, rng)
        if rng.random() < style["lowercase_rate"]:
            text = self._maybe_lowercase(text)
        if rng.random() < style["cutoff_rate"]:
            return self._hard_cutoff(text, rng)
        if rng.random() < style["emoji_rate"]:
            text = self._append_emoji(text, style["emojis"], rng)
        return text

    def generate_reply(self, user_message, author_name=None, token=None):
        api_key = self._get_api_key()
        if not api_key:
            return None
        if not user_message:
            return None
        model = self._get_model()
        system_prompt = self._get_system_prompt()
        if author_name:
            system_prompt = f"{system_prompt}\n\nUser name: {author_name}"
        self._log(f"[Debug] OpenAI request: model={model}.")
        self._log(f"[Debug] OpenAI input length: {len(user_message)}.")

        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        }
        payload = {
            "model": model,
            "input": user_message,
            "instructions": system_prompt,
        }
        timeout = httpx.Timeout(20.0)
        max_retries = 3
        last_error = None
        proxy = resolve_proxy_for_traffic("external", external_proxy=load_external_proxy(self.db)) or None
        for attempt in range(max_retries):
            try:
                with httpx_client(proxy, timeout=timeout) as client:
                    response = client.post(
                        "https://api.openai.com/v1/responses",
                        headers=headers,
                        json=payload,
                    )
            except Exception as exc:
                last_error = exc
                self._log(f"[Debug] OpenAI request exception (attempt {attempt + 1}/{max_retries}): {exc}")
                wait = min(8.0, 1.5 * (2 ** attempt))
                time.sleep(wait)
                continue
            if response.status_code == 200:
                try:
                    data = response.json()
                except Exception:
                    self._log("[AI] OpenAI response parse failed.")
                    return None
                break
            if response.status_code in {429, 500, 502, 503, 504}:
                body_preview = response.text[:200] if response.text else ""
                self._log(f"[AI] OpenAI error: {response.status_code} {body_preview}")
                self._log(f"[Debug] OpenAI retrying (attempt {attempt + 1}/{max_retries}).")
                wait = min(8.0, 1.5 * (2 ** attempt))
                time.sleep(wait)
                continue
            body_preview = response.text[:200] if response.text else ""
            self._log(f"[AI] OpenAI error: {response.status_code} {body_preview}")
            return None
        else:
            if last_error:
                self._log(f"[AI] OpenAI request failed: {last_error}")
                self._log(f"[Error] OpenAI request failed after {max_retries} attempts: {last_error}")
            return None

        output_text = data.get("output_text")
        if isinstance(output_text, str) and output_text.strip():
            self._log(f"[Debug] OpenAI response length: {len(output_text.strip())}.")
            self._log(f"[Info] OpenAI reply generated ({len(output_text.strip())} chars).")
            return self._mutate_reply(output_text.strip(), token)

        chunks = []
        for item in data.get("output", []) or []:
            if not isinstance(item, dict):
                continue
            if item.get("type") != "message":
                continue
            for content in item.get("content", []) or []:
                if not isinstance(content, dict):
                    continue
                if content.get("type") in {"output_text", "text"}:
                    text = content.get("text")
                    if text:
                        chunks.append(text)
        if chunks:
            joined = "\n".join(chunks).strip()
            self._log(f"[Debug] OpenAI response length: {len(joined)}.")
            self._log(f"[Info] OpenAI reply generated ({len(joined)} chars).")
            return self._mutate_reply(joined, token)
        return None

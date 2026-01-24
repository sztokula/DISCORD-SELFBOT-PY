import httpx
import time
from proxy_utils import httpx_client


DEFAULT_MODEL = "gpt-4o-mini"
DEFAULT_SYSTEM_PROMPT = (
    "You are a friendly human chatting on Discord. Reply naturally, keep it short, "
    "and avoid sounding salesy or pushy. Do not mention being an AI."
)


class OpenAIResponder:
    def __init__(self, db_manager, log_callback=None):
        self.db = db_manager
        self.log = log_callback

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

    def generate_reply(self, user_message, author_name=None):
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
        for attempt in range(max_retries):
            try:
                with httpx_client(timeout=timeout) as client:
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
            return output_text.strip()

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
            return joined
        return None

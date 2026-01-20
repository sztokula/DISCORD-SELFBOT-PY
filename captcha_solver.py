import time
from typing import Optional, Tuple

import httpx


class CaptchaSolver:
    SUPPORTED_PROVIDERS = {"capsolver", "2captcha", "anticaptcha"}
    PROVIDER_ALIASES = {
        "capsolver": "capsolver",
        "2captcha": "2captcha",
        "anticaptcha": "anticaptcha",
        "anti-captcha": "anticaptcha",
        "anti captcha": "anticaptcha",
    }

    def __init__(self, db_manager, log_callback):
        self.db = db_manager
        self.log = log_callback

    def _normalize_provider(self, provider: str) -> str:
        normalized = (provider or "").strip().lower()
        return self.PROVIDER_ALIASES.get(normalized, provider)

    def get_provider(self) -> str:
        provider = self._normalize_provider(self.db.get_setting("captcha_provider", "capsolver"))
        if provider not in self.SUPPORTED_PROVIDERS:
            return "capsolver"
        return provider

    def get_api_key(self, provider: Optional[str] = None) -> str:
        provider = self._normalize_provider(provider or self.get_provider())
        key_name = f"{provider}_api_key"
        key = self.db.get_setting(key_name, "")
        if not key and provider == "anticaptcha":
            key = self.db.get_setting("anti-captcha_api_key", "")
        return key

    def check_balance(self, provider: Optional[str] = None, api_key: Optional[str] = None) -> Tuple[bool, str]:
        provider = self._normalize_provider(provider or self.get_provider())
        api_key = api_key or self.get_api_key(provider)
        if not api_key:
            return False, "Missing API key."
        if provider == "capsolver":
            return self._capsolver_balance(api_key)
        if provider == "2captcha":
            return self._twocaptcha_balance(api_key)
        if provider == "anticaptcha":
            return self._anticaptcha_balance(api_key)
        return False, "Unsupported provider."

    def solve_captcha(self, captcha_info: dict, provider: Optional[str] = None, api_key: Optional[str] = None) -> Tuple[bool, str]:
        provider = self._normalize_provider(provider or self.get_provider())
        api_key = api_key or self.get_api_key(provider)
        if not api_key:
            return False, "Missing API key."

        service = (captcha_info.get("service") or "hcaptcha").lower()
        if service in {"hcaptcha", "hcaptcha_enterprise"}:
            return self._solve_hcaptcha(provider, api_key, captcha_info)
        if service in {"funcaptcha", "arkose", "arkoselabs", "arkose-labs"}:
            return self._solve_arkose(provider, api_key, captcha_info)
        return False, f"Unsupported captcha type: {service}"

    def _solve_hcaptcha(self, provider: str, api_key: str, captcha_info: dict) -> Tuple[bool, str]:
        if provider == "capsolver":
            return self._capsolver_hcaptcha(api_key, captcha_info)
        if provider == "2captcha":
            return self._twocaptcha_hcaptcha(api_key, captcha_info)
        if provider == "anticaptcha":
            return self._anticaptcha_hcaptcha(api_key, captcha_info)
        return False, "Unsupported provider."

    def _solve_arkose(self, provider: str, api_key: str, captcha_info: dict) -> Tuple[bool, str]:
        if provider == "capsolver":
            return self._capsolver_arkose(api_key, captcha_info)
        if provider == "2captcha":
            return self._twocaptcha_arkose(api_key, captcha_info)
        if provider == "anticaptcha":
            return self._anticaptcha_arkose(api_key, captcha_info)
        return False, "Unsupported provider."

    def _capsolver_balance(self, api_key: str) -> Tuple[bool, str]:
        try:
            response = httpx.post(
                "https://api.capsolver.com/getBalance",
                json={"clientKey": api_key},
                timeout=httpx.Timeout(10.0),
            )
            data = response.json()
            if data.get("errorId") == 0:
                balance = data.get("balance", "0")
                return True, f"Balance: {balance} USD"
            return False, data.get("errorDescription", "Unknown error")
        except Exception as exc:
            return False, str(exc)

    def _twocaptcha_balance(self, api_key: str) -> Tuple[bool, str]:
        try:
            response = httpx.get(
                "https://2captcha.com/res.php",
                params={"key": api_key, "action": "getbalance", "json": 1},
                timeout=httpx.Timeout(10.0),
            )
            data = response.json()
            if data.get("status") == 1:
                return True, f"Balance: {data.get('request')} USD"
            return False, data.get("request", "Unknown error")
        except Exception as exc:
            return False, str(exc)

    def _anticaptcha_balance(self, api_key: str) -> Tuple[bool, str]:
        try:
            response = httpx.post(
                "https://api.anti-captcha.com/getBalance",
                json={"clientKey": api_key},
                timeout=httpx.Timeout(10.0),
            )
            data = response.json()
            if data.get("errorId") == 0:
                return True, f"Balance: {data.get('balance', '0')} USD"
            return False, data.get("errorDescription", "Unknown error")
        except Exception as exc:
            return False, str(exc)

    def _capsolver_hcaptcha(self, api_key: str, captcha_info: dict) -> Tuple[bool, str]:
        task = {
            "type": "HCaptchaTaskProxyLess",
            "websiteURL": captcha_info["url"],
            "websiteKey": captcha_info["sitekey"],
        }
        rqdata = captcha_info.get("rqdata")
        if rqdata:
            task["enterprisePayload"] = {"rqdata": rqdata}
        return self._capsolver_solve(api_key, task)

    def _capsolver_arkose(self, api_key: str, captcha_info: dict) -> Tuple[bool, str]:
        task = {
            "type": "FunCaptchaTaskProxyLess",
            "websiteURL": captcha_info["url"],
            "websitePublicKey": captcha_info["sitekey"],
        }
        rqdata = captcha_info.get("rqdata")
        if rqdata:
            task["data"] = rqdata
        return self._capsolver_solve(api_key, task)

    def _anticaptcha_hcaptcha(self, api_key: str, captcha_info: dict) -> Tuple[bool, str]:
        task = {
            "type": "HCaptchaTaskProxyless",
            "websiteURL": captcha_info["url"],
            "websiteKey": captcha_info["sitekey"],
        }
        rqdata = captcha_info.get("rqdata")
        if rqdata:
            task["isEnterprise"] = True
            task["enterprisePayload"] = {"rqdata": rqdata}
        return self._anticaptcha_solve(api_key, task)

    def _anticaptcha_arkose(self, api_key: str, captcha_info: dict) -> Tuple[bool, str]:
        task = {
            "type": "FunCaptchaTaskProxyless",
            "websiteURL": captcha_info["url"],
            "websitePublicKey": captcha_info["sitekey"],
        }
        rqdata = captcha_info.get("rqdata")
        if rqdata:
            task["data"] = rqdata
        return self._anticaptcha_solve(api_key, task)

    def _capsolver_solve(self, api_key: str, task: dict) -> Tuple[bool, str]:
        try:
            create = httpx.post(
                "https://api.capsolver.com/createTask",
                json={"clientKey": api_key, "task": task},
                timeout=httpx.Timeout(15.0),
            )
            create_data = create.json()
            if create_data.get("errorId") != 0:
                return False, create_data.get("errorDescription", "Unknown error")
            task_id = create_data.get("taskId")
            if not task_id:
                return False, "Missing taskId from CapSolver."
            return self._capsolver_poll(api_key, task_id)
        except Exception as exc:
            return False, str(exc)

    def _capsolver_poll(self, api_key: str, task_id: str) -> Tuple[bool, str]:
        deadline = time.monotonic() + 120
        while time.monotonic() < deadline:
            time.sleep(3)
            try:
                resp = httpx.post(
                    "https://api.capsolver.com/getTaskResult",
                    json={"clientKey": api_key, "taskId": task_id},
                    timeout=httpx.Timeout(15.0),
                )
                data = resp.json()
            except Exception as exc:
                return False, str(exc)
            if data.get("status") == "ready":
                solution = data.get("solution", {})
                token = (
                    solution.get("gRecaptchaResponse")
                    or solution.get("token")
                    or solution.get("response")
                )
                if token:
                    return True, token
                return False, "Missing captcha token in response."
            if data.get("status") == "failed":
                return False, data.get("errorDescription", "Captcha failed")
        return False, "Captcha wait timeout."

    def _anticaptcha_solve(self, api_key: str, task: dict) -> Tuple[bool, str]:
        try:
            create = httpx.post(
                "https://api.anti-captcha.com/createTask",
                json={"clientKey": api_key, "task": task},
                timeout=httpx.Timeout(15.0),
            )
            create_data = create.json()
            if create_data.get("errorId") != 0:
                return False, create_data.get("errorDescription", "Unknown error")
            task_id = create_data.get("taskId")
            if not task_id:
                return False, "Missing taskId from Anti-Captcha."
            return self._anticaptcha_poll(api_key, task_id)
        except Exception as exc:
            return False, str(exc)

    def _anticaptcha_poll(self, api_key: str, task_id: str) -> Tuple[bool, str]:
        deadline = time.monotonic() + 120
        while time.monotonic() < deadline:
            time.sleep(3)
            try:
                resp = httpx.post(
                    "https://api.anti-captcha.com/getTaskResult",
                    json={"clientKey": api_key, "taskId": task_id},
                    timeout=httpx.Timeout(15.0),
                )
                data = resp.json()
            except Exception as exc:
                return False, str(exc)
            if data.get("errorId") not in (None, 0):
                return False, data.get("errorDescription", "Anti-Captcha failed")
            if data.get("status") == "ready":
                solution = data.get("solution", {})
                token = solution.get("gRecaptchaResponse") or solution.get("token")
                if token:
                    return True, token
                return False, "Missing captcha token in response."
        return False, "Captcha wait timeout."

    def _twocaptcha_hcaptcha(self, api_key: str, captcha_info: dict) -> Tuple[bool, str]:
        params = {
            "key": api_key,
            "method": "hcaptcha",
            "sitekey": captcha_info["sitekey"],
            "pageurl": captcha_info["url"],
            "json": 1,
        }
        rqdata = captcha_info.get("rqdata")
        if rqdata:
            params["data"] = rqdata
        return self._twocaptcha_solve(params)

    def _twocaptcha_arkose(self, api_key: str, captcha_info: dict) -> Tuple[bool, str]:
        params = {
            "key": api_key,
            "method": "funcaptcha",
            "publickey": captcha_info["sitekey"],
            "pageurl": captcha_info["url"],
            "json": 1,
        }
        rqdata = captcha_info.get("rqdata")
        if rqdata:
            params["data[blob]"] = rqdata
        return self._twocaptcha_solve(params)

    def _twocaptcha_solve(self, params: dict) -> Tuple[bool, str]:
        try:
            submit = httpx.get(
                "https://2captcha.com/in.php",
                params=params,
                timeout=httpx.Timeout(15.0),
            )
            data = submit.json()
            if data.get("status") != 1:
                return False, data.get("request", "Unknown error")
            request_id = data.get("request")
        except Exception as exc:
            return False, str(exc)

        deadline = time.monotonic() + 120
        while time.monotonic() < deadline:
            time.sleep(5)
            try:
                res = httpx.get(
                    "https://2captcha.com/res.php",
                    params={"key": params["key"], "action": "get", "id": request_id, "json": 1},
                    timeout=httpx.Timeout(15.0),
                )
                res_data = res.json()
            except Exception as exc:
                return False, str(exc)
            if res_data.get("status") == 1:
                return True, res_data.get("request", "")
            if res_data.get("request") != "CAPCHA_NOT_READY":
                return False, res_data.get("request", "2Captcha error")
        return False, "Captcha wait timeout."

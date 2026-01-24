import time
from typing import Optional, Tuple

import httpx
from proxy_utils import httpx_client, load_external_proxy, resolve_proxy_for_traffic


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

    def _post_json(self, url, *, timeout, **kwargs):
        proxy = resolve_proxy_for_traffic("external", external_proxy=load_external_proxy(self.db))
        with httpx_client(proxy or None, timeout=timeout) as client:
            response = client.post(url, **kwargs)
            return response.json()

    def _get_json(self, url, *, timeout, **kwargs):
        proxy = resolve_proxy_for_traffic("external", external_proxy=load_external_proxy(self.db))
        with httpx_client(proxy or None, timeout=timeout) as client:
            response = client.get(url, **kwargs)
            return response.json()

    @staticmethod
    def _get_field(captcha_info: dict, *keys):
        for key in keys:
            if key in captcha_info:
                value = captcha_info.get(key)
                if value not in (None, ""):
                    return value
        return None

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

        service = (captcha_info.get("service") or "hcaptcha").lower().strip().replace("-", "_")
        if service in {"hcaptcha", "hcaptcha_enterprise"}:
            return self._solve_hcaptcha(provider, api_key, captcha_info)
        if service in {"funcaptcha", "arkose", "arkoselabs", "arkose_labs"}:
            return self._solve_arkose(provider, api_key, captcha_info)
        if service in {
            "recaptcha",
            "recaptcha_v2",
            "recaptcha_v2_invisible",
            "recaptcha_v3",
            "recaptcha_enterprise",
            "recaptcha_v2_enterprise",
            "recaptcha_v3_enterprise",
            "recaptcha_enterprise_v2",
            "recaptcha_enterprise_v3",
        }:
            return self._solve_recaptcha(provider, api_key, captcha_info, service)
        if service in {"turnstile", "cloudflare_turnstile"}:
            return self._solve_turnstile(provider, api_key, captcha_info)
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

    def _solve_recaptcha(self, provider: str, api_key: str, captcha_info: dict, service: str) -> Tuple[bool, str]:
        service = service.lower()
        version = (self._get_field(captcha_info, "version", "recaptcha_version") or "").lower()
        min_score = self._get_field(captcha_info, "min_score", "minScore")
        action = self._get_field(captcha_info, "action", "pageAction", "page_action")
        is_v3 = service in {"recaptcha_v3", "recaptcha_v3_enterprise", "recaptcha_enterprise_v3"} or version == "v3" or min_score is not None
        is_enterprise = "enterprise" in service or bool(self._get_field(captcha_info, "enterprise", "isEnterprise"))
        is_invisible = "invisible" in service or bool(self._get_field(captcha_info, "invisible", "isInvisible"))

        if provider == "capsolver":
            return self._capsolver_recaptcha(api_key, captcha_info, is_v3, is_enterprise, is_invisible, action)
        if provider == "2captcha":
            return self._twocaptcha_recaptcha(
                api_key,
                captcha_info,
                is_v3=is_v3,
                is_enterprise=is_enterprise,
                is_invisible=is_invisible,
                action=action,
                min_score=min_score,
            )
        if provider == "anticaptcha":
            return self._anticaptcha_recaptcha(api_key, captcha_info, is_v3, is_enterprise, is_invisible, action, min_score)
        return False, "Unsupported provider."

    def _solve_turnstile(self, provider: str, api_key: str, captcha_info: dict) -> Tuple[bool, str]:
        if provider == "capsolver":
            return self._capsolver_turnstile(api_key, captcha_info)
        if provider == "2captcha":
            return self._twocaptcha_turnstile(api_key, captcha_info)
        if provider == "anticaptcha":
            return self._anticaptcha_turnstile(api_key, captcha_info)
        return False, "Unsupported provider."

    def _capsolver_balance(self, api_key: str) -> Tuple[bool, str]:
        try:
            data = self._post_json(
                "https://api.capsolver.com/getBalance",
                json={"clientKey": api_key},
                timeout=httpx.Timeout(10.0),
            )
            if data.get("errorId") == 0:
                balance = data.get("balance", "0")
                return True, f"Balance: {balance} USD"
            return False, data.get("errorDescription", "Unknown error")
        except Exception as exc:
            return False, str(exc)

    def _twocaptcha_balance(self, api_key: str) -> Tuple[bool, str]:
        try:
            data = self._get_json(
                "https://2captcha.com/res.php",
                params={"key": api_key, "action": "getbalance", "json": 1},
                timeout=httpx.Timeout(10.0),
            )
            if data.get("status") == 1:
                return True, f"Balance: {data.get('request')} USD"
            return False, data.get("request", "Unknown error")
        except Exception as exc:
            return False, str(exc)

    def _anticaptcha_balance(self, api_key: str) -> Tuple[bool, str]:
        try:
            data = self._post_json(
                "https://api.anti-captcha.com/getBalance",
                json={"clientKey": api_key},
                timeout=httpx.Timeout(10.0),
            )
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
            task["rqdata"] = rqdata
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
            task["rqdata"] = rqdata
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
            task["rqdata"] = rqdata
        return self._anticaptcha_solve(api_key, task)

    def _capsolver_solve(self, api_key: str, task: dict) -> Tuple[bool, str]:
        try:
            create_data = self._post_json(
                "https://api.capsolver.com/createTask",
                json={"clientKey": api_key, "task": task},
                timeout=httpx.Timeout(15.0),
            )
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
                data = self._post_json(
                    "https://api.capsolver.com/getTaskResult",
                    json={"clientKey": api_key, "taskId": task_id},
                    timeout=httpx.Timeout(15.0),
                )
            except Exception as exc:
                return False, str(exc)
            if data.get("status") == "ready":
                solution = data.get("solution", {})
                user_agent = solution.get("userAgent") or solution.get("useragent")
                token = (
                    solution.get("gRecaptchaResponse")
                    or solution.get("token")
                    or solution.get("response")
                )
                if token:
                    if user_agent:
                        return True, {"token": token, "userAgent": user_agent}
                    return True, token
                return False, "Missing captcha token in response."
            if data.get("status") == "failed":
                return False, data.get("errorDescription", "Captcha failed")
        return False, "Captcha wait timeout."

    def _anticaptcha_solve(self, api_key: str, task: dict) -> Tuple[bool, str]:
        try:
            create_data = self._post_json(
                "https://api.anti-captcha.com/createTask",
                json={"clientKey": api_key, "task": task},
                timeout=httpx.Timeout(15.0),
            )
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
                data = self._post_json(
                    "https://api.anti-captcha.com/getTaskResult",
                    json={"clientKey": api_key, "taskId": task_id},
                    timeout=httpx.Timeout(15.0),
                )
            except Exception as exc:
                return False, str(exc)
            if data.get("errorId") not in (None, 0):
                return False, data.get("errorDescription", "Anti-Captcha failed")
            if data.get("status") == "ready":
                solution = data.get("solution", {})
                user_agent = solution.get("userAgent") or solution.get("useragent")
                token = solution.get("gRecaptchaResponse") or solution.get("token")
                if token:
                    if user_agent:
                        return True, {"token": token, "userAgent": user_agent}
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
            params["rqdata"] = rqdata
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
            params["rqdata"] = rqdata
        surl = captcha_info.get("surl") or captcha_info.get("api_server")
        if surl:
            params["surl"] = surl
        return self._twocaptcha_solve(params)

    def _twocaptcha_recaptcha(
        self,
        api_key: str,
        captcha_info: dict,
        *,
        is_v3: bool,
        is_enterprise: bool,
        is_invisible: bool,
        action: Optional[str],
        min_score: Optional[object],
    ) -> Tuple[bool, str]:
        params = {
            "key": api_key,
            "method": "userrecaptcha",
            "googlekey": captcha_info["sitekey"],
            "pageurl": captcha_info["url"],
            "json": 1,
        }
        rqdata = captcha_info.get("rqdata")
        if rqdata:
            params["rqdata"] = rqdata
        if is_v3:
            params["version"] = "v3"
            if min_score is None:
                min_score = "0.3"
            params["min_score"] = min_score
            if action:
                params["action"] = action
        else:
            if is_invisible:
                params["invisible"] = 1
        if is_enterprise:
            params["enterprise"] = 1
        data_s = self._get_field(captcha_info, "data_s", "data-s", "recaptchaDataSValue")
        if data_s:
            params["data-s"] = data_s
        cookies = self._get_field(captcha_info, "cookies")
        if cookies:
            params["cookies"] = cookies
        user_agent = self._get_field(captcha_info, "userAgent", "user_agent")
        if user_agent:
            params["userAgent"] = user_agent
        api_domain = self._get_field(captcha_info, "domain", "apiDomain", "api_domain")
        if api_domain:
            params["domain"] = api_domain
        return self._twocaptcha_solve(params)

    def _twocaptcha_turnstile(self, api_key: str, captcha_info: dict) -> Tuple[bool, str]:
        params = {
            "key": api_key,
            "method": "turnstile",
            "sitekey": captcha_info["sitekey"],
            "pageurl": captcha_info["url"],
            "json": 1,
        }
        rqdata = captcha_info.get("rqdata")
        if rqdata:
            params["rqdata"] = rqdata
        action = self._get_field(captcha_info, "action", "pageAction", "page_action")
        if action:
            params["action"] = action
        data = self._get_field(captcha_info, "data", "cdata", "cData")
        if data:
            params["data"] = data
        pagedata = self._get_field(captcha_info, "pagedata", "pageData", "chlPageData", "chl_page_data")
        if pagedata:
            params["pagedata"] = pagedata
        user_agent = self._get_field(captcha_info, "userAgent", "user_agent")
        if user_agent:
            params["userAgent"] = user_agent
        return self._twocaptcha_solve(params)

    def _capsolver_recaptcha(
        self,
        api_key: str,
        captcha_info: dict,
        is_v3: bool,
        is_enterprise: bool,
        is_invisible: bool,
        action: Optional[str],
    ) -> Tuple[bool, str]:
        if is_v3:
            task_type = "ReCaptchaV3EnterpriseTaskProxyLess" if is_enterprise else "ReCaptchaV3TaskProxyLess"
        else:
            task_type = "ReCaptchaV2EnterpriseTaskProxyLess" if is_enterprise else "ReCaptchaV2TaskProxyLess"
        task = {
            "type": task_type,
            "websiteURL": captcha_info["url"],
            "websiteKey": captcha_info["sitekey"],
        }
        if is_invisible and not is_v3:
            task["isInvisible"] = True
        if action:
            task["pageAction"] = action
        data_s = self._get_field(captcha_info, "data_s", "data-s", "recaptchaDataSValue", "s")
        if data_s:
            payload = task.get("enterprisePayload") or {}
            payload["s"] = data_s
            task["enterprisePayload"] = payload
        rqdata = captcha_info.get("rqdata")
        if rqdata:
            payload = task.get("enterprisePayload") or {}
            payload["rqdata"] = rqdata
            task["enterprisePayload"] = payload
        api_domain = self._get_field(captcha_info, "apiDomain", "api_domain", "domain")
        if api_domain:
            task["apiDomain"] = api_domain
        return self._capsolver_solve(api_key, task)

    def _capsolver_turnstile(self, api_key: str, captcha_info: dict) -> Tuple[bool, str]:
        task = {
            "type": "AntiTurnstileTaskProxyLess",
            "websiteURL": captcha_info["url"],
            "websiteKey": captcha_info["sitekey"],
        }
        metadata = {}
        rqdata = captcha_info.get("rqdata")
        if rqdata:
            metadata["rqdata"] = rqdata
        action = self._get_field(captcha_info, "action", "pageAction", "page_action")
        if action:
            metadata["action"] = action
        cdata = self._get_field(captcha_info, "cdata", "cData", "data")
        if cdata:
            metadata["cdata"] = cdata
        if metadata:
            task["metadata"] = metadata
        return self._capsolver_solve(api_key, task)

    def _anticaptcha_recaptcha(
        self,
        api_key: str,
        captcha_info: dict,
        is_v3: bool,
        is_enterprise: bool,
        is_invisible: bool,
        action: Optional[str],
        min_score: Optional[object],
    ) -> Tuple[bool, str]:
        if is_v3:
            task = {
                "type": "RecaptchaV3TaskProxyless",
                "websiteURL": captcha_info["url"],
                "websiteKey": captcha_info["sitekey"],
                "minScore": float(min_score) if min_score is not None else 0.3,
            }
            if action:
                task["pageAction"] = action
            if is_enterprise:
                task["isEnterprise"] = True
        else:
            task_type = "RecaptchaV2EnterpriseTaskProxyless" if is_enterprise else "RecaptchaV2TaskProxyless"
            task = {
                "type": task_type,
                "websiteURL": captcha_info["url"],
                "websiteKey": captcha_info["sitekey"],
            }
            if is_invisible:
                task["isInvisible"] = True
            data_s = self._get_field(captcha_info, "data_s", "data-s", "recaptchaDataSValue", "s")
            if data_s:
                if is_enterprise:
                    task["enterprisePayload"] = {"s": data_s}
                else:
                    task["recaptchaDataSValue"] = data_s
            api_domain = self._get_field(captcha_info, "apiDomain", "api_domain", "domain")
            if api_domain:
                task["apiDomain"] = api_domain
        rqdata = captcha_info.get("rqdata")
        if rqdata:
            payload = task.get("enterprisePayload") or {}
            payload["rqdata"] = rqdata
            task["enterprisePayload"] = payload
        return self._anticaptcha_solve(api_key, task)

    def _anticaptcha_turnstile(self, api_key: str, captcha_info: dict) -> Tuple[bool, str]:
        task = {
            "type": "TurnstileTaskProxyless",
            "websiteURL": captcha_info["url"],
            "websiteKey": captcha_info["sitekey"],
        }
        rqdata = captcha_info.get("rqdata")
        if rqdata:
            task["rqdata"] = rqdata
        action = self._get_field(captcha_info, "action", "pageAction", "page_action")
        if action:
            task["action"] = action
        cdata = self._get_field(captcha_info, "cdata", "cData", "data")
        if cdata:
            task["cData"] = cdata
        pagedata = self._get_field(captcha_info, "pagedata", "pageData", "chlPageData", "chl_page_data")
        if pagedata:
            task["chlPageData"] = pagedata
        return self._anticaptcha_solve(api_key, task)

    def _twocaptcha_solve(self, params: dict) -> Tuple[bool, str]:
        try:
            data = self._get_json(
                "https://2captcha.com/in.php",
                params=params,
                timeout=httpx.Timeout(15.0),
            )
            if data.get("status") != 1:
                return False, data.get("request", "Unknown error")
            request_id = data.get("request")
        except Exception as exc:
            return False, str(exc)

        deadline = time.monotonic() + 120
        while time.monotonic() < deadline:
            time.sleep(5)
            try:
                res_data = self._get_json(
                    "https://2captcha.com/res.php",
                    params={"key": params["key"], "action": "get", "id": request_id, "json": 1},
                    timeout=httpx.Timeout(15.0),
                )
            except Exception as exc:
                return False, str(exc)
            if res_data.get("status") == 1:
                token = res_data.get("request", "")
                user_agent = res_data.get("useragent") or res_data.get("userAgent")
                if user_agent:
                    return True, {"token": token, "userAgent": user_agent}
                return True, token
            if res_data.get("request") != "CAPCHA_NOT_READY":
                return False, res_data.get("request", "2Captcha error")
        return False, "Captcha wait timeout."

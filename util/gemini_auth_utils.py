"""
Gemini Business 认证工具类
整合用户脚本的稳健逻辑，支持多种邮箱 API 格式

包含：
- 邮箱 API 兼容层（支持 /api/generate-email 和 /admin/new_address）
- 稳健的验证码输入（6格/单格/OTP 三种模式）
- 禁用"重新发送"按钮防误触
- HTML 验证码解析
"""
import json
import time
import re
import logging
from typing import Optional, Dict, Any, List
from urllib.parse import urlparse, parse_qs
from datetime import datetime

import requests
from bs4 import BeautifulSoup
from core.config import config

logger = logging.getLogger("gemini.auth_utils")


class GeminiAuthConfig:
    """认证配置类（从统一配置模块加载）"""

    def __init__(self):
        # 从统一配置模块读取
        self.mail_api = config.basic.mail_api
        self.admin_key = config.basic.mail_admin_key
        self.email_domains = config.basic.email_domain  # 改为数组
        self.google_mail = config.basic.google_mail
        self.login_url = config.security.login_url

    def validate(self) -> bool:
        """验证配置是否完整"""
        required = [self.mail_api, self.admin_key, self.login_url]
        return all(required)


class GeminiAuthHelper:
    """Gemini 认证辅助工具"""

    # XPath 配置（公共）
    XPATH = {
        "email_input": "/html/body/c-wiz/div/div/div[1]/div/div/div/form/div[1]/div[1]/div/span[2]/input",
        "continue_btn": "/html/body/c-wiz/div/div/div[1]/div/div/div/form/div[2]/div/button",
        "verify_btn": "/html/body/c-wiz/div/div/div[1]/div/div/div/form/div[2]/div/div[1]/span/div[1]/button",
    }

    # CSS 选择器备用
    SELECTORS = {
        "email_input": [
            "input[type='email']",
            "input[name='email']",
            "input[autocomplete='email']",
            "input[autocomplete='username']",
            "input[aria-label*='mail']",
            "form input[type='text']",
        ],
        "continue_btn": [
            "button[type='submit']",
            "button[data-action='continue']",
            "button[aria-label*='Continue']",
        ],
    }

    def __init__(self, auth_config: GeminiAuthConfig):
        self.config = auth_config

    # ==================== 邮箱 API 兼容层 ====================

    def create_email(self, domain: Optional[str] = None) -> Optional[str]:
        """
        创建临时邮箱（兼容多种 API 格式）
        
        支持的 API 格式：
        1. /api/generate-email (用户脚本格式，自动生成邮箱，不需要配置域名)
        2. /admin/new_address (项目原格式，需要配置域名)
        """
        if not self.config.mail_api or not self.config.admin_key:
            logger.error("🔴 [CONFIG] 邮箱 API 未配置")
            return None

        # 方式1: /api/generate-email (推荐，自动生成邮箱地址)
        try:
            r = requests.get(
                f"{self.config.mail_api}/api/generate-email",
                headers={"X-API-Key": self.config.admin_key},
                timeout=30,
                verify=False
            )
            if r.status_code == 200:
                data = r.json()
                if data.get('success'):
                    email = data.get('data', {}).get('email')
                    if email:
                        logger.info(f"✅ 邮箱创建成功: {email}")
                        return email
                # 提取可能的错误信息
                error_msg = data.get('message') or data.get('error') or '未知错误'
                logger.warning(f"⚠️ generate-email API 返回失败: {error_msg}")
        except requests.exceptions.RequestException as e:
            logger.warning(f"⚠️ generate-email API 请求失败: {e}")
        except Exception as e:
            logger.warning(f"⚠️ generate-email API 解析失败: {e}")

        # 方式2: /admin/new_address (备选，需要域名配置)
        if self.config.email_domains:
            try:
                import random
                from string import ascii_letters, digits
                
                if not domain:
                    domain = random.choice(self.config.email_domains)
                
                name = ''.join(random.sample(ascii_letters + digits, 10))
                
                r = requests.post(
                    f"{self.config.mail_api}/admin/new_address",
                    headers={"x-admin-auth": self.config.admin_key},
                    json={"enablePrefix": False, "name": name, "domain": domain},
                    timeout=30,
                    verify=False
                )
                if r.status_code == 200:
                    email = r.json().get('address')
                    if email:
                        logger.info(f"✅ 邮箱创建成功 (备选API): {email}")
                        return email
            except Exception as e:
                logger.error(f"🔴 [EMAIL] 备选API创建邮箱失败: {e}")
        else:
            logger.debug("未配置 email_domains，跳过备选 API")

        logger.error("🔴 [EMAIL] 所有邮箱创建方式均失败")
        return None

    def _extract_code_from_html(self, content: str) -> Optional[str]:
        """从 HTML 邮件内容中提取 6 位验证码"""
        if not content:
            return None

        soup = BeautifulSoup(content, "html.parser")

        # 方法 1: 查找 class 包含 verification-code 的元素
        for class_pattern in ["verification-code", "verification_code", "code", "otp", "pin"]:
            elements = soup.find_all(class_=lambda x: x and class_pattern in str(x).lower())
            for el in elements:
                text = el.get_text().strip()
                if re.match(r'^[A-Za-z0-9]{6}$', text):
                    return text

        # 方法 2: 查找大号字体/粗体中的 6 位码
        for tag in ['strong', 'b', 'h1', 'h2', 'h3', 'span', 'div', 'p', 'td']:
            for el in soup.find_all(tag):
                text = el.get_text().strip()
                if re.match(r'^[A-Za-z0-9]{6}$', text):
                    return text

        # 方法 3: 正则匹配纯文本中的验证码
        plain_text = soup.get_text() if soup else content
        patterns = [
            r'(?:code|Code|CODE|verification)[:\s]+([A-Za-z0-9]{6})\b',
            r'\b([0-9]{6})\b',
            r'\b([A-Z0-9]{6})\b',
            r'\b([A-Za-z0-9]{6})\b',
        ]
        for pattern in patterns:
            match = re.search(pattern, plain_text)
            if match:
                return match.group(1)

        return None

    def get_verification_code(self, email: str, timeout: int = 60, old_email_id: Optional[str] = None) -> Optional[str]:
        """
        获取验证码（兼容多种 API 格式）
        
        支持的 API 格式：
        1. /api/emails?email={email} (用户脚本格式，HTML解析)
        2. /admin/mails (项目原格式，ai_extract)
        """
        logger.info(f"⏳ 等待验证码 [{email}]...")
        start = time.time()

        # 先获取当前最新邮件ID，用于判断是否有新邮件
        if old_email_id is None:
            try:
                r = requests.get(
                    f"{self.config.mail_api}/api/emails",
                    params={"email": email},
                    headers={"X-API-Key": self.config.admin_key},
                    timeout=10,
                    verify=False
                )
                if r.status_code == 200:
                    data = r.json()
                    emails = []
                    if isinstance(data, dict):
                        emails = data.get("data", {}).get("emails", []) or data.get("emails", [])
                    elif isinstance(data, list):
                        emails = data
                    if emails:
                        old_email_id = emails[0].get("id")
            except Exception:
                pass

        while time.time() - start < timeout:
            # 尝试方式1: /api/emails (用户脚本格式)
            try:
                r = requests.get(
                    f"{self.config.mail_api}/api/emails",
                    params={"email": email},
                    headers={"X-API-Key": self.config.admin_key},
                    timeout=10,
                    verify=False
                )
                if r.status_code == 200:
                    data = r.json()
                    emails = []
                    if isinstance(data, dict):
                        emails = data.get("data", {}).get("emails", []) or data.get("emails", []) or data.get("messages", [])
                    elif isinstance(data, list):
                        emails = data

                    if emails:
                        latest_email = emails[0]
                        new_email_id = latest_email.get("id")
                        
                        # 检查是否是新邮件
                        if old_email_id and new_email_id == old_email_id:
                            time.sleep(2)
                            continue

                        # 尝试从 HTML 解析验证码
                        content = (
                            latest_email.get("html_content") or
                            latest_email.get("html") or
                            latest_email.get("body") or
                            latest_email.get("content") or
                            latest_email.get("text") or
                            ""
                        )
                        code = self._extract_code_from_html(content)
                        if code:
                            logger.info(f"✅ 验证码获取成功: {code}")
                            return code
            except Exception:
                pass

            # 尝试方式2: /admin/mails (项目原格式)
            try:
                r = requests.get(
                    f"{self.config.mail_api}/admin/mails?limit=20&offset=0",
                    headers={"x-admin-auth": self.config.admin_key},
                    timeout=10,
                    verify=False
                )
                if r.status_code == 200:
                    emails = r.json().get('results', [])
                    for mail in emails:
                        if mail.get("address") == email and mail.get("source") == self.config.google_mail:
                            try:
                                metadata = json.loads(mail.get("metadata", "{}"))
                                code = metadata.get("ai_extract", {}).get("result")
                                if code:
                                    logger.info(f"✅ 验证码获取成功: {code}")
                                    return code
                            except Exception:
                                pass
            except Exception:
                pass

            time.sleep(2)

        logger.error(f"🔴 [TIMEOUT] 验证码超时 [{email}]")
        return None

    # ==================== 稳健的输入函数 ====================

    def _dispatch_input_change(self, driver, element):
        """触发 input/change 事件"""
        try:
            driver.execute_script(
                "arguments[0].dispatchEvent(new Event('input', {bubbles: true}));"
                "arguments[0].dispatchEvent(new Event('change', {bubbles: true}));",
                element,
            )
        except Exception:
            pass

    def _get_input_value(self, driver, element) -> str:
        """可靠地读取输入框的值"""
        try:
            v = element.get_property("value")
            if v is not None:
                return str(v)
        except Exception:
            pass
        try:
            v = element.get_attribute("value")
            if v is not None:
                return str(v)
        except Exception:
            pass
        try:
            v = driver.execute_script("return arguments[0].value;", element)
            if v is not None:
                return str(v)
        except Exception:
            pass
        return ""

    def clear_and_type(self, driver, element, text: str, delay: float = 0.03, attempts: int = 5, require_match: bool = True) -> bool:
        """
        稳健的输入函数：点击聚焦、清空、逐字输入；用 JS/Property 校验
        """
        from selenium.webdriver.common.keys import Keys
        
        last_value = ""
        text = str(text)
        
        for _ in range(max(1, attempts)):
            try:
                driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", element)
            except Exception:
                pass

            try:
                element.click()
            except Exception:
                try:
                    driver.execute_script("arguments[0].click();", element)
                except Exception:
                    pass
            time.sleep(0.15)

            # 清空：Ctrl+A + Backspace 通常比 clear() 更可靠
            try:
                element.send_keys(Keys.CONTROL, 'a')
                element.send_keys(Keys.BACKSPACE)
            except Exception:
                try:
                    element.clear()
                except Exception:
                    pass
            time.sleep(0.05)

            for ch in text:
                element.send_keys(ch)
                if delay:
                    time.sleep(delay)

            self._dispatch_input_change(driver, element)

            last_value = self._get_input_value(driver, element).strip()
            if last_value == text:
                return True

            # 兜底：JS 直接赋值
            try:
                driver.execute_script("arguments[0].value = arguments[1];", element, text)
                self._dispatch_input_change(driver, element)
                time.sleep(0.05)
                last_value = self._get_input_value(driver, element).strip()
                if last_value == text:
                    return True
            except Exception:
                pass

            # 再兜底：tab/blur 后再读一次
            try:
                element.send_keys(Keys.TAB)
                time.sleep(0.05)
                last_value = self._get_input_value(driver, element).strip()
                if last_value == text:
                    return True
            except Exception:
                pass

            time.sleep(0.2)

        if require_match:
            logger.warning(f"⚠️ 输入校验失败，期望: {text} 实际: {last_value}")
            return False

        return True

    # ==================== 禁用重新发送按钮 ====================

    def disable_resend_buttons(self, driver) -> int:
        """禁用"重新发送/Resend"按钮，避免误触导致验证码作废"""
        js = r"""
        const deny = ['重新发送','Resend','Send again','Try again','重新获取'];
        const buttons = Array.from(document.querySelectorAll('button'));
        let count = 0;
        for (const b of buttons) {
            const t = (b.innerText || '').trim();
            if (!t) continue;
            const hit = deny.some(d => t.toLowerCase().includes(d.toLowerCase()));
            if (hit) {
                try {
                    b.disabled = true;
                    b.setAttribute('aria-disabled', 'true');
                    b.style.pointerEvents = 'none';
                    b.style.opacity = '0.5';
                    count++;
                } catch (e) {}
            }
        }
        return count;
        """
        try:
            return int(driver.execute_script(js) or 0)
        except Exception:
            return 0

    # ==================== 验证码输入 ====================

    def fill_verification_code(self, driver, wait, code: str) -> bool:
        """
        稳健填入验证码：兼容单输入框/6个输入框/OTP容器三种形态
        (从用户脚本移植)
        """
        from selenium.webdriver.common.by import By
        from selenium.webdriver.common.keys import Keys
        from selenium.webdriver.support import expected_conditions as EC
        
        code = str(code).strip()
        logger.debug(f"[DEBUG] 开始填入验证码: {code}")

        # 再次禁用重新发送按钮
        self.disable_resend_buttons(driver)

        if len(code) != 6:
            logger.error(f"🔴 [VERIFY_FAIL] 验证码长度不是6位: {len(code)}")
            return False

        def _poll_typed(pins, expected, timeout=2.0):
            start = time.time()
            while time.time() - start < timeout:
                try:
                    last = "".join((p.get_attribute("value") or "") for p in pins[:6])
                except Exception:
                    last = ""
                if last == expected:
                    return True
                time.sleep(0.1)
            return False

        # 1) 如果有 6 个 pinInput 输入框，逐个填
        try:
            pins = driver.find_elements(By.CSS_SELECTOR, "input[name='pinInput']")
            if len(pins) >= 6:
                for attempt in range(3):
                    for i, ch in enumerate(code[:6]):
                        try:
                            pins[i].click()
                        except Exception:
                            pass
                        time.sleep(0.05)
                        try:
                            pins[i].send_keys(Keys.CONTROL, 'a')
                            pins[i].send_keys(Keys.BACKSPACE)
                        except Exception:
                            try:
                                pins[i].clear()
                            except Exception:
                                pass
                        pins[i].send_keys(ch)
                        time.sleep(0.10 if attempt == 0 else 0.14)

                    if _poll_typed(pins, code, timeout=2.0):
                        logger.debug("[DEBUG] 验证码输入成功 (6格)")
                        return True

                    # 兜底：点第一个格子后整体发送
                    try:
                        pins[0].click()
                        time.sleep(0.1)
                        driver.switch_to.active_element.send_keys(code)
                    except Exception:
                        pass

                    if _poll_typed(pins, code, timeout=2.0):
                        return True

                logger.warning("⚠️ 验证码输入后未能读到完整值(可能是属性不同/渲染延迟)，继续尝试")
                return True
        except Exception as e:
            logger.debug(f"[DEBUG] 多格输入异常: {e}")

        # 2) 单个输入框
        try:
            pin = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, "input[name='pinInput']")))
            try:
                pin.click()
            except Exception:
                pass
            time.sleep(0.1)
            try:
                pin.send_keys(Keys.CONTROL, 'a')
                pin.send_keys(Keys.BACKSPACE)
            except Exception:
                try:
                    pin.clear()
                except Exception:
                    pass
            time.sleep(0.05)
            pin.send_keys(code)

            start = time.time()
            while time.time() - start < 2.0:
                val = (pin.get_attribute("value") or "").strip()
                if val == code:
                    logger.debug("[DEBUG] 验证码输入成功 (单格)")
                    return True
                time.sleep(0.1)

            # 兜底：JS 赋值
            try:
                driver.execute_script("arguments[0].value = arguments[1];", pin, code)
                self._dispatch_input_change(driver, pin)
                time.sleep(0.2)
                val = (pin.get_attribute("value") or "").strip()
                if val == code:
                    return True
            except Exception:
                pass

            logger.warning("⚠️ 验证码输入后未能读取到 value(可能为自定义组件)，继续尝试")
            return True
        except Exception as e:
            logger.debug(f"[DEBUG] 单输入框异常: {e}")

        # 3) OTP 容器
        try:
            first = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, "span[data-index='0']")))
            first.click()
            time.sleep(0.2)
            driver.switch_to.active_element.send_keys(code)
            time.sleep(0.2)

            pins = driver.find_elements(By.CSS_SELECTOR, "input[name='pinInput']")
            if len(pins) >= 6:
                if _poll_typed(pins, code, timeout=2.0):
                    return True
            return True
        except Exception as e:
            logger.debug(f"[DEBUG] OTP 容器异常: {e}")

        # 4) 最后兜底：发给当前焦点
        try:
            driver.switch_to.active_element.send_keys(code)
            return True
        except Exception:
            return False

    # ==================== 点击按钮 ====================

    def click_primary_action(self, driver, timeout: int = 3) -> bool:
        """点击主要操作按钮（继续/下一步/登录等），避开重新发送"""
        texts = [
            "继续", "下一步", "登录", "确认", "提交", "完成",
            "Continue", "Next", "Sign in", "Log in", "Submit", "Done", "Create",
        ]
        deny = ["重新发送", "Resend", "Send again", "Try again", "重新获取"]
        
        from selenium.webdriver.common.by import By
        
        end = time.time() + timeout
        while time.time() < end:
            try:
                for btn in driver.find_elements(By.CSS_SELECTOR, "button[type='submit']"):
                    try:
                        if not btn.is_displayed() or not btn.is_enabled():
                            continue
                        t = (btn.text or "").strip()
                        if any(d.lower() in t.lower() for d in deny):
                            continue
                        if any(k.lower() in t.lower() for k in texts):
                            driver.execute_script("arguments[0].click();", btn)
                            return True
                    except Exception:
                        continue
            except Exception:
                pass

            try:
                for b in driver.find_elements(By.TAG_NAME, "button"):
                    try:
                        if not b.is_displayed() or not b.is_enabled():
                            continue
                        t = (b.text or "").strip()
                        if not t:
                            continue
                        if any(d.lower() in t.lower() for d in deny):
                            continue
                        for key in texts:
                            if key.lower() in t.lower():
                                driver.execute_script("arguments[0].click();", b)
                                return True
                    except Exception:
                        continue
            except Exception:
                pass
            time.sleep(0.2)
        return False

    def click_verify_only(self, driver, timeout: int = 3) -> bool:
        """只点击验证/Verify，避开重新发送/Resend"""
        allow = ["验证", "Verify"]
        deny = ["重新发送", "Resend", "Send again", "Try again", "重新获取"]
        
        from selenium.webdriver.common.by import By
        
        end = time.time() + timeout
        while time.time() < end:
            try:
                for b in driver.find_elements(By.TAG_NAME, "button"):
                    try:
                        if not b.is_displayed() or not b.is_enabled():
                            continue
                        t = (b.text or "").strip()
                        if not t:
                            continue
                        if any(d.lower() in t.lower() for d in deny):
                            continue
                        if any(a.lower() in t.lower() for a in allow):
                            driver.execute_script("arguments[0].click();", b)
                            return True
                    except Exception:
                        continue
            except Exception:
                pass
            time.sleep(0.2)
        return False

    # ==================== 邮箱验证流程 ====================

    def perform_email_verification(self, driver, wait, email: str) -> Dict[str, Any]:
        """
        执行邮箱验证流程（公共方法）
        从输入邮箱到验证码验证完成

        返回: {"success": bool, "error": str|None}
        """
        from selenium.webdriver.common.by import By
        from selenium.webdriver.support import expected_conditions as EC

        try:
            # 1. 输入邮箱（优先 XPath，备用 CSS）
            inp = None
            try:
                inp = wait.until(EC.element_to_be_clickable((By.XPATH, self.XPATH["email_input"])))
            except Exception:
                for selector in self.SELECTORS["email_input"]:
                    try:
                        inp = wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, selector)))
                        if inp and inp.is_displayed():
                            break
                    except Exception:
                        continue

            if not inp:
                return {"success": False, "error": "未找到邮箱输入框"}

            if not self.clear_and_type(driver, inp, email, delay=0.03, attempts=3):
                return {"success": False, "error": "邮箱输入失败"}

            # 2. 点击继续
            time.sleep(0.5)
            try:
                btn = wait.until(EC.element_to_be_clickable((By.XPATH, self.XPATH["continue_btn"])))
                driver.execute_script("arguments[0].click();", btn)
            except Exception:
                if not self.click_primary_action(driver, timeout=5):
                    return {"success": False, "error": "点击继续按钮失败"}

            # 3. 禁用重新发送按钮
            time.sleep(1)
            self.disable_resend_buttons(driver)

            # 4. 获取验证码
            time.sleep(1)
            code = self.get_verification_code(email)
            if not code:
                return {"success": False, "error": "验证码超时"}

            # 5. 输入验证码
            time.sleep(0.8)
            self.disable_resend_buttons(driver)
            if not self.fill_verification_code(driver, wait, code):
                return {"success": False, "error": "验证码输入失败"}

            # 6. 再次禁用（输入后可能会重新渲染）
            self.disable_resend_buttons(driver)

            # 7. 点击验证按钮
            time.sleep(0.4)
            try:
                vbtn = driver.find_element(By.XPATH, self.XPATH["verify_btn"])
                driver.execute_script("arguments[0].click();", vbtn)
            except Exception:
                self.click_verify_only(driver, timeout=3)

            return {"success": True, "error": None}

        except Exception as e:
            return {"success": False, "error": str(e)}

    # ==================== 配置提取 ====================

    def extract_config_from_driver(self, driver, email: str, timeout: int = 15) -> Optional[Dict[str, Any]]:
        """从浏览器轮询提取配置（cookies + URL）"""
        start = time.time()
        while time.time() - start < timeout:
            url = driver.current_url
            parsed = urlparse(url)
            csesidx = parse_qs(parsed.query).get("csesidx", [None])[0]

            config_id = None
            parts = url.split("/")
            for i, p in enumerate(parts):
                if p == "cid" and i + 1 < len(parts):
                    config_id = parts[i + 1].split("?")[0]
                    break

            cookies = driver.get_cookies()
            cookie_dict = {c.get("name"): c for c in cookies}
            ses_cookie = cookie_dict.get("__Secure-C_SES", {})
            host_cookie = cookie_dict.get("__Host-C_OSES", {})

            if ses_cookie.get("value") and host_cookie.get("value") and csesidx and config_id:
                expires_at = None
                if ses_cookie.get("expiry"):
                    expires_at = datetime.fromtimestamp(ses_cookie["expiry"] - 43200).strftime("%Y-%m-%d %H:%M:%S")

                return {
                    "id": email,
                    "csesidx": str(csesidx),
                    "config_id": str(config_id),
                    "secure_c_ses": ses_cookie.get("value"),
                    "host_c_oses": host_cookie.get("value"),
                    "expires_at": expires_at,
                }

            time.sleep(0.5)

        # 失败时记录缺失字段
        url = driver.current_url
        parsed = urlparse(url)
        csesidx = parse_qs(parsed.query).get("csesidx", [None])[0]
        cookies = driver.get_cookies()
        cookie_dict = {c.get("name"): c for c in cookies}

        missing = []
        if not cookie_dict.get("__Secure-C_SES", {}).get("value"):
            missing.append("secure_c_ses")
        if not cookie_dict.get("__Host-C_OSES", {}).get("value"):
            missing.append("host_c_oses")
        if not csesidx:
            missing.append("csesidx")
        
        parts = url.split("/")
        has_config_id = any(p == "cid" for p in parts)
        if not has_config_id:
            missing.append("config_id")

        logger.error(f"🔴 [EXTRACT_FAIL] 配置提取失败，缺失字段: {', '.join(missing)}")
        return None

    def extract_config_from_workspace(self, driver) -> Dict[str, Any]:
        """从工作台页面提取配置信息"""
        try:
            time.sleep(3)
            cookies = driver.get_cookies()
            url = driver.current_url
            parsed = urlparse(url)

            path_parts = url.split('/')
            config_id = None
            for i, p in enumerate(path_parts):
                if p == 'cid' and i + 1 < len(path_parts):
                    config_id = path_parts[i + 1].split('?')[0]
                    break

            cookie_dict = {c['name']: c for c in cookies}
            ses_cookie = cookie_dict.get('__Secure-C_SES', {})
            host_cookie = cookie_dict.get('__Host-C_OSES', {})
            csesidx = parse_qs(parsed.query).get('csesidx', [None])[0]

            if not all([ses_cookie.get('value'), host_cookie.get('value'), csesidx, config_id]):
                return {"success": False, "config": None, "error": "配置数据不完整"}

            config_data = {
                "csesidx": csesidx,
                "config_id": config_id,
                "secure_c_ses": ses_cookie.get('value'),
                "host_c_oses": host_cookie.get('value'),
                "expires_at": datetime.fromtimestamp(
                    ses_cookie.get('expiry', 0) - 43200
                ).strftime('%Y-%m-%d %H:%M:%S') if ses_cookie.get('expiry') else None
            }

            return {"success": True, "config": config_data, "error": None}

        except Exception as e:
            return {"success": False, "config": None, "error": str(e)}

    # ==================== 等待工作台 ====================

    def wait_for_workspace(self, driver, timeout: int = 30, max_crash_retries: int = 3) -> bool:
        """等待进入工作台（带崩溃检测）"""
        crash_count = 0
        workspace_url = "https://business.gemini.google/"

        for _ in range(timeout):
            time.sleep(1)
            try:
                page_source = driver.page_source
                is_crashed = 'crashed' in page_source.lower() or 'aw, snap' in page_source.lower()

                if is_crashed:
                    crash_count += 1
                    logger.warning(f"⚠️ 页面崩溃 (崩溃 {crash_count}/{max_crash_retries})")
                    if crash_count >= max_crash_retries:
                        logger.error("🔴 [CRASH] 页面崩溃次数过多，放弃重试")
                        return False

                    if self._recover_from_crash(driver, workspace_url):
                        time.sleep(3)
                        continue
                    else:
                        return False

                url = driver.current_url
                if 'business.gemini.google' in url and '/cid/' in url:
                    return True

            except Exception as e:
                error_msg = str(e).lower()
                if 'crash' in error_msg or 'tab' in error_msg or 'target window' in error_msg:
                    crash_count += 1
                    if crash_count >= max_crash_retries:
                        return False
                    if self._recover_from_crash(driver, workspace_url):
                        time.sleep(3)
                        continue
                    else:
                        return False

        return False

    def _recover_from_crash(self, driver, target_url: str) -> bool:
        """从崩溃中恢复：开新标签页访问目标URL"""
        try:
            original_handles = driver.window_handles
            driver.execute_script("window.open('');")
            time.sleep(0.5)

            new_handles = driver.window_handles
            new_handle = None
            for handle in new_handles:
                if handle not in original_handles:
                    new_handle = handle
                    break

            if not new_handle:
                logger.error("🔴 [CRASH] 无法创建新标签页")
                return False

            driver.switch_to.window(new_handle)

            for handle in original_handles:
                try:
                    driver.switch_to.window(handle)
                    driver.close()
                except Exception:
                    pass

            driver.switch_to.window(new_handle)
            driver.get(target_url)
            time.sleep(3)

            logger.info("✅ 已通过新标签页恢复")
            return True

        except Exception as e:
            logger.error(f"🔴 [CRASH] 恢复失败: {e}")
            return False

    def extract_config_with_retry(self, driver, max_retries: int = 3) -> Dict[str, Any]:
        """带重试机制的配置提取"""
        last_error = None

        for attempt in range(max_retries):
            try:
                page_source = driver.page_source
                if 'crashed' in page_source.lower() or 'aw, snap' in page_source.lower():
                    logger.warning(f"⚠️ 页面崩溃，尝试刷新 ({attempt + 1}/{max_retries})")
                    driver.refresh()
                    time.sleep(3)
                    continue

                extract_result = self.extract_config_from_workspace(driver)
                if extract_result["success"]:
                    return extract_result
                else:
                    last_error = extract_result["error"]
                    logger.warning(f"⚠️ 提取配置失败: {last_error} ({attempt + 1}/{max_retries})")
                    driver.refresh()
                    time.sleep(3)

            except Exception as e:
                error_msg = str(e).lower()
                if 'crash' in error_msg or 'tab' in error_msg:
                    logger.warning(f"⚠️ 检测到页面崩溃: {e} ({attempt + 1}/{max_retries})")
                    try:
                        driver.refresh()
                        time.sleep(3)
                    except Exception:
                        try:
                            driver.get("https://business.gemini.google/")
                            time.sleep(5)
                        except Exception:
                            pass
                else:
                    last_error = str(e)
                    try:
                        driver.refresh()
                        time.sleep(3)
                    except Exception:
                        pass

        return {"success": False, "config": None, "error": last_error or "提取配置失败（已重试）"}

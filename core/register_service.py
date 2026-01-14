"""
Gemini Business 注册服务
将 gemini_register.py 的 Selenium 注册逻辑封装为异步服务

艹，这个SB模块需要 Chrome 环境才能跑，别在没 Chrome 的容器里调用
"""
import asyncio
import json
import os
import time
import random
import logging
import uuid
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from pathlib import Path
from string import ascii_letters, digits
from typing import Optional, List, Dict, Any

import requests
from dotenv import load_dotenv

from util.gemini_auth_utils import GeminiAuthConfig, GeminiAuthHelper

# 加载环境变量
load_dotenv()

logger = logging.getLogger("gemini.register")


class RegisterStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"


@dataclass
class RegisterTask:
    """注册任务"""
    id: str
    count: int
    status: RegisterStatus = RegisterStatus.PENDING
    progress: int = 0
    success_count: int = 0
    fail_count: int = 0
    created_at: float = field(default_factory=time.time)
    finished_at: Optional[float] = None
    results: List[Dict[str, Any]] = field(default_factory=list)
    error: Optional[str] = None

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "count": self.count,
            "status": self.status.value,
            "progress": self.progress,
            "success_count": self.success_count,
            "fail_count": self.fail_count,
            "created_at": datetime.fromtimestamp(self.created_at).isoformat(),
            "finished_at": datetime.fromtimestamp(self.finished_at).isoformat() if self.finished_at else None,
            "results": self.results,
            "error": self.error
        }


class RegisterService:
    """注册服务 - 管理注册任务"""

    # 姓名池
    NAMES = [
        "James Smith", "John Johnson", "Robert Williams", "Michael Brown", "William Jones",
        "David Garcia", "Mary Miller", "Patricia Davis", "Jennifer Rodriguez", "Linda Martinez"
    ]

    def __init__(self):
        self._executor = ThreadPoolExecutor(max_workers=1)
        self._tasks: Dict[str, RegisterTask] = {}
        self._current_task_id: Optional[str] = None
        self._email_queue: List[str] = []
        # 数据目录配置（与 main.py 保持一致）
        if os.path.exists("/data"):
            self.output_dir = Path("/data")
        else:
            self.output_dir = Path("./data")

        # 注意：不再在这里缓存 auth_config，改用 property 动态获取最新配置
        # 这样前端修改邮箱配置后热更新能立即生效
        pass

        # 指定的域名（用于批量注册时指定域名）
        self._specified_domain: Optional[str] = None

    @property
    def auth_config(self) -> GeminiAuthConfig:
        """每次访问时动态获取最新配置，支持热更新"""
        return GeminiAuthConfig()

    @property
    def auth_helper(self) -> GeminiAuthHelper:
        """每次访问时动态获取最新配置，支持热更新"""
        return GeminiAuthHelper(self.auth_config)
    
    @staticmethod
    def _random_str(n: int = 10) -> str:
        """生成随机字符串"""
        return "".join(random.sample(ascii_letters + digits, n))
    
    def _create_email(self, domain: Optional[str] = None) -> Optional[str]:
        """
        创建临时邮箱

        Args:
            domain: 指定域名，如果为 None 则从配置的域名数组随机选择
        """
        if not self.auth_config.mail_api or not self.auth_config.admin_key:
            logger.error("❌ 邮箱 API 未配置")
            return None

        if not self.auth_config.email_domains:
            logger.error("❌ 邮箱域名未配置")
            return None

        try:
            # 如果未指定域名，从域名数组中随机选择一个
            if not domain:
                domain = random.choice(self.auth_config.email_domains)

            json_data = {
                "enablePrefix": False,
                "name": self._random_str(10),
                "domain": domain
            }
            r = requests.post(
                f"{self.auth_config.mail_api}/admin/new_address",
                headers={"x-admin-auth": self.auth_config.admin_key},
                json=json_data,
                timeout=30,
                verify=False
            )
            if r.status_code == 200:
                return r.json()['address']
        except Exception as e:
            logger.error(f"❌ 创建邮箱失败: {e}")
        return None

    def _get_email(self) -> Optional[str]:
        """获取邮箱（优先从队列取，否则创建新邮箱）"""
        if self._email_queue:
            return self._email_queue.pop(0)
        return self._create_email(self._specified_domain)
    
    def _save_config(self, email: str, data: dict) -> Optional[dict]:
        """保存账户配置到 accounts.json"""
        self.output_dir.mkdir(parents=True, exist_ok=True)
        accounts_file = self.output_dir / "accounts.json"

        config = {
            "id": email,
            "csesidx": data["csesidx"],
            "config_id": data["config_id"],
            "secure_c_ses": data["secure_c_ses"],
            "host_c_oses": data["host_c_oses"],
            "expires_at": data.get("expires_at")
        }

        # 读取现有配置
        accounts = []
        if accounts_file.exists():
            try:
                with open(accounts_file, 'r') as f:
                    accounts = json.load(f)
            except:
                accounts = []

        # 追加新账户配置
        accounts.append(config)

        # 保存配置
        with open(accounts_file, 'w') as f:
            json.dump(accounts, f, indent=2, ensure_ascii=False)

        logger.info(f"✅ 配置已保存到 accounts.json: {email}")
        return config
    
    def _register_one_sync(self) -> Dict[str, Any]:
        """
        同步执行单次注册 (在线程池中运行)
        返回: {"email": str, "success": bool, "config": dict|None, "error": str|None}
        """
        try:
            # 延迟导入 selenium，因为可能没装
            import undetected_chromedriver as uc
            from selenium.webdriver.common.by import By
            from selenium.webdriver.support.ui import WebDriverWait
            from selenium.webdriver.support import expected_conditions as EC
            from selenium.webdriver.common.keys import Keys
        except ImportError as e:
            return {"email": None, "success": False, "config": None, "error": f"Selenium 未安装: {e}"}
        
        email = self._get_email()
        if not email:
            return {"email": None, "success": False, "config": None, "error": "无法创建邮箱"}

        driver = None
        try:
            logger.info(f"🚀 开始注册: {email}")
            
            # 配置 Chrome 选项（增加稳定性，减少崩溃）
            options = uc.ChromeOptions()
            options.add_argument('--no-sandbox')
            options.add_argument('--disable-dev-shm-usage')
            options.add_argument('--disable-gpu')
            options.add_argument('--disable-software-rasterizer')
            options.add_argument('--disable-extensions')
            options.add_argument('--window-size=1920,1080')
            # 增加内存限制，避免崩溃
            options.add_argument('--js-flags=--max-old-space-size=512')
            # 禁用一些可能导致崩溃的特性
            options.add_argument('--disable-background-networking')
            options.add_argument('--disable-default-apps')
            options.add_argument('--disable-sync')
            
            driver = uc.Chrome(options=options, use_subprocess=True)
            wait = WebDriverWait(driver, 30)

            # 1. 访问登录页
            driver.get(self.auth_config.login_url)
            time.sleep(2)

            # 2-6. 执行邮箱验证流程（使用公共方法）
            verify_result = self.auth_helper.perform_email_verification(driver, wait, email)
            if not verify_result["success"]:
                return {"email": email, "success": False, "config": None, "error": verify_result["error"]}
            
            # 7. 输入姓名
            time.sleep(2)
            selectors = [
                "input[formcontrolname='fullName']",
                "input[placeholder='全名']",
                "input[placeholder='Full name']",
                "input#mat-input-0",
            ]
            name_inp = None
            for _ in range(30):
                for sel in selectors:
                    try:
                        name_inp = driver.find_element(By.CSS_SELECTOR, sel)
                        if name_inp.is_displayed():
                            break
                    except:
                        continue
                if name_inp and name_inp.is_displayed():
                    break
                time.sleep(1)
            
            if name_inp and name_inp.is_displayed():
                name = random.choice(self.NAMES)
                name_inp.click()
                time.sleep(0.2)
                name_inp.clear()
                for c in name:
                    name_inp.send_keys(c)
                    time.sleep(0.02)
                time.sleep(0.3)
                name_inp.send_keys(Keys.ENTER)
                time.sleep(1)
            else:
                return {"email": email, "success": False, "config": None, "error": "未找到姓名输入框"}
            
            # 8. 等待进入工作台（使用公共方法）
            if not self.auth_helper.wait_for_workspace(driver, timeout=30):
                return {"email": email, "success": False, "config": None, "error": "未跳转到工作台"}

            # 9. 提取配置（使用公共方法，带重试机制处理 tab crashed）
            extract_result = self.auth_helper.extract_config_with_retry(driver, max_retries=3)
            if not extract_result["success"]:
                return {"email": email, "success": False, "config": None, "error": extract_result["error"]}

            config_data = extract_result["config"]
            
            config = self._save_config(email, config_data)
            logger.info(f"✅ 注册成功: {email}")
            return {"email": email, "success": True, "config": config, "error": None}
            
        except Exception as e:
            logger.error(f"❌ 注册异常 [{email}]: {e}")
            return {"email": email, "success": False, "config": None, "error": str(e)}
        finally:
            if driver:
                try:
                    driver.quit()
                except:
                    pass
    
    async def start_register(self, count: int, domain: Optional[str] = None) -> RegisterTask:
        """
        启动注册任务

        Args:
            count: 注册数量
            domain: 指定域名，为 None 则随机选择
        """
        if self._current_task_id:
            current_task = self._tasks.get(self._current_task_id)
            if current_task and current_task.status == RegisterStatus.RUNNING:
                raise ValueError("已有注册任务在运行中")

        # 设置指定的域名
        self._specified_domain = domain

        task = RegisterTask(
            id=str(uuid.uuid4()),
            count=count
        )
        self._tasks[task.id] = task
        self._current_task_id = task.id
        
        # 在后台线程执行注册
        asyncio.create_task(self._run_register_async(task))
        
        return task
    
    async def _run_register_async(self, task: RegisterTask):
        """异步执行注册任务"""
        task.status = RegisterStatus.RUNNING
        loop = asyncio.get_event_loop()
        
        try:
            for i in range(task.count):
                task.progress = i + 1
                result = await loop.run_in_executor(self._executor, self._register_one_sync)
                task.results.append(result)
                
                if result["success"]:
                    task.success_count += 1
                else:
                    task.fail_count += 1
                
                # 每次注册间隔
                if i < task.count - 1:
                    await asyncio.sleep(random.randint(2, 5))
            
            task.status = RegisterStatus.SUCCESS if task.success_count > 0 else RegisterStatus.FAILED
        except Exception as e:
            task.status = RegisterStatus.FAILED
            task.error = str(e)
        finally:
            task.finished_at = time.time()
            self._current_task_id = None
    
    def get_task(self, task_id: str) -> Optional[RegisterTask]:
        """获取任务状态"""
        return self._tasks.get(task_id)
    
    def get_current_task(self) -> Optional[RegisterTask]:
        """获取当前运行的任务"""
        if self._current_task_id:
            return self._tasks.get(self._current_task_id)
        return None


# 全局注册服务实例
_register_service: Optional[RegisterService] = None


def get_register_service() -> RegisterService:
    """获取全局注册服务"""
    global _register_service
    if _register_service is None:
        _register_service = RegisterService()
    return _register_service

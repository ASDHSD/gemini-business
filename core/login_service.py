"""
Gemini Business 登录刷新服务
用于刷新即将过期的账户配置

整合用户脚本的稳健逻辑，添加 60 秒超时保护
"""
import asyncio
import json
import os
import time
import logging
import uuid
import threading
from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from enum import Enum
from pathlib import Path
from typing import Optional, List, Dict, Any

from dotenv import load_dotenv

from util.gemini_auth_utils import GeminiAuthConfig, GeminiAuthHelper

# 加载环境变量
load_dotenv()

logger = logging.getLogger("gemini.login")


class LoginStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"


@dataclass
class LoginTask:
    """登录刷新任务"""
    id: str
    account_ids: List[str]  # 需要刷新的账户ID列表
    status: LoginStatus = LoginStatus.PENDING
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
            "account_ids": self.account_ids,
            "status": self.status.value,
            "progress": self.progress,
            "success_count": self.success_count,
            "fail_count": self.fail_count,
            "created_at": datetime.fromtimestamp(self.created_at).isoformat(),
            "finished_at": datetime.fromtimestamp(self.finished_at).isoformat() if self.finished_at else None,
            "results": self.results,
            "error": self.error
        }


class TimeoutException(Exception):
    """超时异常"""
    pass


def run_with_timeout(func, args=(), kwargs=None, timeout_seconds=60):
    """
    使用线程实现超时保护（兼容 Windows，因为 signal.SIGALRM 只在 Unix 有效）
    """
    kwargs = kwargs or {}
    result = [None]
    exception = [None]
    
    def target():
        try:
            result[0] = func(*args, **kwargs)
        except Exception as e:
            exception[0] = e
    
    thread = threading.Thread(target=target)
    thread.daemon = True
    thread.start()
    thread.join(timeout=timeout_seconds)
    
    if thread.is_alive():
        # 超时了
        raise TimeoutException(f"操作超时 (>{timeout_seconds}s)")
    
    if exception[0]:
        raise exception[0]
    
    return result[0]


class LoginService:
    """登录刷新服务 - 管理账户刷新任务"""

    # 单账户超时时间（秒）
    ACCOUNT_TIMEOUT = 60

    def __init__(self):
        self._executor = ThreadPoolExecutor(max_workers=1)
        self._tasks: Dict[str, LoginTask] = {}
        self._current_task_id: Optional[str] = None
        # 数据目录配置（与 main.py 保持一致）
        if os.path.exists("/data"):
            self.output_dir = Path("/data")
        else:
            self.output_dir = Path("./data")
        self._polling_task: Optional[asyncio.Task] = None
        self._is_polling = False

    @property
    def auth_config(self) -> GeminiAuthConfig:
        """每次访问时动态获取最新配置，支持热更新"""
        return GeminiAuthConfig()

    @property
    def auth_helper(self) -> GeminiAuthHelper:
        """每次访问时动态获取最新配置，支持热更新"""
        return GeminiAuthHelper(self.auth_config)

    def _update_account_config(self, email: str, data: dict) -> Optional[dict]:
        """更新账户配置到 accounts.json"""
        accounts_file = self.output_dir / "accounts.json"

        # 读取现有配置
        accounts = []
        if accounts_file.exists():
            try:
                with open(accounts_file, 'r') as f:
                    accounts = json.load(f)
            except Exception:
                accounts = []

        # 查找并更新对应账户
        updated = False
        for account in accounts:
            if account.get("id") == email:
                account["csesidx"] = data["csesidx"]
                account["config_id"] = data["config_id"]
                account["secure_c_ses"] = data["secure_c_ses"]
                account["host_c_oses"] = data["host_c_oses"]
                account["expires_at"] = data.get("expires_at")
                updated = True
                break

        if not updated:
            logger.warning(f"⚠️ 账户 {email} 不存在于 accounts.json，跳过更新")
            return None

        # 保存配置
        with open(accounts_file, 'w') as f:
            json.dump(accounts, f, indent=2, ensure_ascii=False)

        logger.info(f"✅ 配置已更新: {email}")
        return data

    def _login_one_sync_inner(self, email: str) -> Dict[str, Any]:
        """
        同步执行单次登录刷新（内部方法，会被超时包装）
        整合 refresh_gemini_accounts.py 的逻辑
        """
        try:
            # 延迟导入 selenium
            import undetected_chromedriver as uc
            from selenium.webdriver.common.by import By
            from selenium.webdriver.support.ui import WebDriverWait
            from selenium.webdriver.support import expected_conditions as EC
            from selenium.webdriver.common.keys import Keys
        except ImportError as e:
            return {"email": email, "success": False, "config": None, "error": f"Selenium 未安装: {e}"}

        driver = None
        try:
            logger.info(f"🔄 开始刷新登录: {email}")

            # 配置 Chrome 选项
            options = uc.ChromeOptions()
            options.add_argument('--no-sandbox')
            options.add_argument('--disable-dev-shm-usage')
            options.add_argument('--disable-gpu')
            options.add_argument('--disable-software-rasterizer')
            options.add_argument('--disable-extensions')
            options.add_argument('--window-size=1920,1080')
            options.add_argument('--js-flags=--max-old-space-size=512')
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
                logger.error(f"🔴 [VERIFY_FAIL] {email} 验证失败: {verify_result['error']}")
                return {"email": email, "success": False, "config": None, "error": verify_result["error"]}

            # 7. 等待进入工作台
            if not self.auth_helper.wait_for_workspace(driver, timeout=30):
                logger.error(f"🔴 [WORKSPACE_FAIL] {email} 未跳转到工作台")
                return {"email": email, "success": False, "config": None, "error": "未跳转到工作台"}

            # 8. 提取配置
            config_data = self.auth_helper.extract_config_from_driver(driver, email, timeout=15)
            if not config_data:
                logger.error(f"🔴 [EXTRACT_FAIL] {email} 配置提取失败")
                return {"email": email, "success": False, "config": None, "error": "配置提取失败"}

            config = self._update_account_config(email, config_data)
            logger.info(f"✅ 登录刷新成功: {email}")
            return {"email": email, "success": True, "config": config, "error": None}

        except Exception as e:
            logger.error(f"🔴 [ERROR] 登录刷新异常 [{email}]: {e}")
            return {"email": email, "success": False, "config": None, "error": str(e)}
        finally:
            if driver:
                try:
                    driver.quit()
                except Exception:
                    pass

    def _login_one_sync(self, email: str) -> Dict[str, Any]:
        """
        同步执行单次登录刷新（带 60 秒超时保护）
        """
        try:
            return run_with_timeout(
                self._login_one_sync_inner,
                args=(email,),
                timeout_seconds=self.ACCOUNT_TIMEOUT
            )
        except TimeoutException:
            logger.error(f"🔴 [TIMEOUT] {email} 刷新超时(>{self.ACCOUNT_TIMEOUT}s)，已跳过")
            return {"email": email, "success": False, "config": None, "error": f"超时(>{self.ACCOUNT_TIMEOUT}s)"}
        except Exception as e:
            logger.error(f"🔴 [ERROR] {email} 刷新异常: {e}")
            return {"email": email, "success": False, "config": None, "error": str(e)}

    async def start_login(self, account_ids: List[str]) -> LoginTask:
        """启动登录刷新任务"""
        if self._current_task_id:
            current_task = self._tasks.get(self._current_task_id)
            if current_task and current_task.status == LoginStatus.RUNNING:
                raise ValueError("已有登录刷新任务在运行中")

        task = LoginTask(
            id=str(uuid.uuid4()),
            account_ids=account_ids
        )
        self._tasks[task.id] = task
        self._current_task_id = task.id

        # 在后台线程执行登录刷新
        asyncio.create_task(self._run_login_async(task))

        return task

    async def _run_login_async(self, task: LoginTask):
        """异步执行登录刷新任务"""
        task.status = LoginStatus.RUNNING
        loop = asyncio.get_event_loop()

        try:
            for i, account_id in enumerate(task.account_ids):
                task.progress = i + 1
                logger.info(f"📋 刷新进度: {task.progress}/{len(task.account_ids)} - {account_id}")
                
                result = await loop.run_in_executor(self._executor, self._login_one_sync, account_id)
                task.results.append(result)

                if result["success"]:
                    task.success_count += 1
                else:
                    task.fail_count += 1

                # 每次刷新间隔
                if i < len(task.account_ids) - 1:
                    await asyncio.sleep(2)

            task.status = LoginStatus.SUCCESS if task.success_count > 0 else LoginStatus.FAILED
        except Exception as e:
            task.status = LoginStatus.FAILED
            task.error = str(e)
            logger.error(f"🔴 [TASK_FAIL] 刷新任务异常: {e}")
        finally:
            task.finished_at = time.time()
            self._current_task_id = None
            logger.info(f"📊 刷新任务完成: 成功 {task.success_count}, 失败 {task.fail_count}")

    def get_task(self, task_id: str) -> Optional[LoginTask]:
        """获取任务状态"""
        return self._tasks.get(task_id)

    def get_current_task(self) -> Optional[LoginTask]:
        """获取当前运行的任务"""
        if self._current_task_id:
            return self._tasks.get(self._current_task_id)
        return None

    def _get_expiring_accounts(self) -> List[str]:
        """获取1小时内即将过期的账户ID列表"""
        accounts_file = self.output_dir / "accounts.json"

        if not accounts_file.exists():
            return []

        try:
            with open(accounts_file, 'r') as f:
                accounts = json.load(f)
        except Exception:
            return []

        expiring = []
        beijing_tz = timezone(timedelta(hours=8))
        now = datetime.now(beijing_tz)

        for account in accounts:
            expires_at = account.get("expires_at")
            if not expires_at:
                continue

            try:
                expire_time = datetime.strptime(expires_at, "%Y-%m-%d %H:%M:%S")
                expire_time = expire_time.replace(tzinfo=beijing_tz)
                remaining = (expire_time - now).total_seconds() / 3600

                # 1小时内即将过期
                if 0 < remaining <= 1:
                    expiring.append(account.get("id"))
            except Exception:
                continue

        return expiring

    async def check_and_refresh(self):
        """检查并刷新即将过期的账户"""
        expiring_accounts = self._get_expiring_accounts()

        if not expiring_accounts:
            logger.debug("[LOGIN] 没有需要刷新的账户")
            return

        logger.info(f"🔔 发现 {len(expiring_accounts)} 个账户即将过期，开始刷新")

        try:
            task = await self.start_login(expiring_accounts)
            logger.info(f"📋 刷新任务已创建: {task.id}")
        except ValueError as e:
            logger.warning(f"⚠️ {e}")

    async def start_polling(self):
        """启动轮询任务（每30分钟检查一次）"""
        if self._is_polling:
            logger.warning("⚠️ 轮询任务已在运行中")
            return

        self._is_polling = True
        logger.info("🚀 账户过期检查轮询已启动（间隔: 30分钟）")

        try:
            while self._is_polling:
                await self.check_and_refresh()
                await asyncio.sleep(1800)  # 30分钟
        except asyncio.CancelledError:
            logger.info("⏹️ 轮询任务已停止")
        except Exception as e:
            logger.error(f"🔴 [POLLING_FAIL] 轮询任务异常: {e}")
        finally:
            self._is_polling = False

    def stop_polling(self):
        """停止轮询任务"""
        self._is_polling = False
        logger.info("⏹️ 正在停止轮询任务...")


# 全局登录服务实例
_login_service: Optional[LoginService] = None


def get_login_service() -> LoginService:
    """获取全局登录服务"""
    global _login_service
    if _login_service is None:
        _login_service = LoginService()
    return _login_service

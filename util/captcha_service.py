"""
YesCaptcha 打码服务
用于解决 reCAPTCHA v3 验证分数过低的问题（仅在刷新 Cookie 时使用）
"""
import logging
import time
import requests
from typing import Optional

logger = logging.getLogger("gemini.captcha")

# Gemini Business 验证页面的 reCAPTCHA 配置
WEBSITE_KEY = '6Ld8dCcrAAAAAFVbDMVZy8aNRwCjakBVaDEdRUH8'
WEBSITE_URL = 'https://accountverification.business.gemini.google'


class YesCaptchaService:
    """YesCaptcha 打码服务"""
    
    def __init__(self, api_key: str = None):
        if api_key:
            self.api_key = api_key
        else:
            # 延迟导入避免循环依赖
            from core.config import config
            self.api_key = config.basic.yescaptcha_api_key
    
    @property
    def is_enabled(self) -> bool:
        """检查服务是否已配置"""
        return bool(self.api_key)
    
    def get_recaptcha_token(self, page_action: str = "verify_oob_code", timeout: int = 60) -> Optional[str]:
        """
        获取 reCAPTCHA v3 Token
        
        Args:
            page_action: reCAPTCHA 的 pageAction 参数
            timeout: 超时时间（秒）
            
        Returns:
            成功返回 Token 字符串，失败返回 None
        """
        if not self.is_enabled:
            logger.warning("⚠️ YesCaptcha API Key 未配置")
            return None
        
        try:
            logger.info("🤖 正在向 YesCaptcha 请求 reCAPTCHA Token...")
            
            # 创建任务
            create_resp = requests.post(
                'https://api.yescaptcha.com/createTask',
                json={
                    'clientKey': self.api_key,
                    'task': {
                        'websiteURL': WEBSITE_URL,
                        'websiteKey': WEBSITE_KEY,
                        'pageAction': page_action,
                        'type': 'RecaptchaV3TaskProxylessM1'
                    }
                },
                timeout=10
            )
            create_data = create_resp.json()
            
            if create_data.get('errorId', 0) != 0:
                logger.error(f"❌ YesCaptcha 创建任务失败: {create_data.get('errorDescription')}")
                return None
            
            task_id = create_data.get('taskId')
            if not task_id:
                logger.error("❌ YesCaptcha 未返回 taskId")
                return None
            
            logger.info(f"📋 YesCaptcha 任务已创建: {task_id}")
            
            # 轮询结果
            start_time = time.time()
            while time.time() - start_time < timeout:
                time.sleep(3)
                
                result_resp = requests.post(
                    'https://api.yescaptcha.com/getTaskResult',
                    json={
                        'clientKey': self.api_key,
                        'taskId': task_id
                    },
                    timeout=10
                )
                result_data = result_resp.json()
                
                if result_data.get('status') == 'ready':
                    token = result_data.get('solution', {}).get('gRecaptchaResponse')
                    if token:
                        logger.info("✅ YesCaptcha Token 获取成功")
                        return token
                    
                elif result_data.get('errorId', 0) != 0:
                    logger.error(f"❌ YesCaptcha 获取结果失败: {result_data.get('errorDescription')}")
                    return None
            
            logger.error("❌ YesCaptcha 获取 Token 超时")
            return None
            
        except Exception as e:
            logger.error(f"❌ YesCaptcha 请求异常: {e}")
            return None


# 全局服务实例
_captcha_service: Optional[YesCaptchaService] = None


def get_captcha_service() -> YesCaptchaService:
    """获取全局打码服务实例"""
    global _captcha_service
    if _captcha_service is None:
        _captcha_service = YesCaptchaService()
    return _captcha_service


def reset_captcha_service():
    """重置打码服务（配置更新后调用）"""
    global _captcha_service
    _captcha_service = None

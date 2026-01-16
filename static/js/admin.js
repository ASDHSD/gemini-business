let currentConfig = null;

// 复制到剪贴板函数
function copyToClipboard(text, button) {
    navigator.clipboard.writeText(text).then(() => {
        // 显示复制成功状态
        const originalText = button.innerHTML;
        button.innerHTML = '✓';
        button.classList.add('copied');

        // 2秒后恢复
        setTimeout(() => {
            button.innerHTML = originalText;
            button.classList.remove('copied');
        }, 2000);
    }).catch(err => {
        console.error('复制失败:', err);
        alert('复制失败，请手动复制');
    });
}

// 标签页切换函数
function switchTab(tabName) {
    // 隐藏所有标签页内容
    document.querySelectorAll('.tab-content').forEach(content => {
        content.classList.remove('active');
    });
    // 移除所有标签按钮的active状态
    document.querySelectorAll('.tab-button').forEach(button => {
        button.classList.remove('active');
    });

    // 显示选中的标签页
    document.getElementById('tab-' + tabName).classList.add('active');
    // 激活对应的按钮
    event.target.classList.add('active');
}

// 统一的页面刷新函数（避免缓存）
function refreshPage() {
    window.location.reload();
}

// 统一的错误处理函数
async function handleApiResponse(response) {
    if (!response.ok) {
        const errorText = await response.text();
        let errorMsg;
        try {
            const errorJson = JSON.parse(errorText);
            errorMsg = errorJson.detail || errorJson.message || errorText;
        } catch {
            errorMsg = errorText;
        }
        throw new Error(`HTTP $${response.status}: $${errorMsg}`);
    }
    return await response.json();
}

async function showEditConfig() {
    const config = await fetch(`/${window.ADMIN_PATH}/accounts-config`).then(r => r.json());
    currentConfig = config.accounts;
    const json = JSON.stringify(config.accounts, null, 2);
    document.getElementById('jsonEditor').value = json;
    document.getElementById('jsonError').classList.remove('show');
    document.getElementById('jsonModal').classList.add('show');

    // 实时验证 JSON
    document.getElementById('jsonEditor').addEventListener('input', validateJSON);
}

function validateJSON() {
    const editor = document.getElementById('jsonEditor');
    const errorDiv = document.getElementById('jsonError');
    try {
        JSON.parse(editor.value);
        errorDiv.classList.remove('show');
        errorDiv.textContent = '';
        return true;
    } catch (e) {
        errorDiv.classList.add('show');
        errorDiv.textContent = '❌ JSON 格式错误: ' + e.message;
        return false;
    }
}

function closeModal() {
    document.getElementById('jsonModal').classList.remove('show');
    document.getElementById('jsonEditor').removeEventListener('input', validateJSON);
}

async function saveConfig() {
    if (!validateJSON()) {
        alert('JSON 格式错误，请修正后再保存');
        return;
    }

    const newJson = document.getElementById('jsonEditor').value;
    const originalJson = JSON.stringify(currentConfig, null, 2);

    if (newJson === originalJson) {
        closeModal();
        return;
    }

    try {
        const data = JSON.parse(newJson);
        const response = await fetch(`/${window.ADMIN_PATH}/accounts-config`, {
            method: 'PUT',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(data)
        });

        await handleApiResponse(response);
        closeModal();
        refreshPage();
    } catch (error) {
        console.error('保存失败:', error);
        alert('更新失败: ' + error.message);
    }
}

async function deleteAccount(accountId) {
    if (!confirm(`确定删除账户 $${accountId}？`)) return;

    try {
        const response = await fetch(`/${window.ADMIN_PATH}/accounts/${accountId}`, {
            method: 'DELETE'
        });

        await handleApiResponse(response);
        refreshPage();
    } catch (error) {
        console.error('删除失败:', error);
        alert('删除失败: ' + error.message);
    }
}

async function disableAccount(accountId) {
    try {
        const response = await fetch(`/${window.ADMIN_PATH}/accounts/${accountId}/disable`, {
            method: 'PUT'
        });

        await handleApiResponse(response);
        refreshPage();
    } catch (error) {
        console.error('禁用失败:', error);
        alert('禁用失败: ' + error.message);
    }
}

async function enableAccount(accountId) {
    try {
        const response = await fetch(`/${window.ADMIN_PATH}/accounts/${accountId}/enable`, {
            method: 'PUT'
        });

        await handleApiResponse(response);
        refreshPage();
    } catch (error) {
        console.error('启用失败:', error);
        alert('启用失败: ' + error.message);
    }
}

// 批量上传相关函数
async function handleFileUpload(event) {
    const files = event.target.files;
    if (!files.length) return;

    let newAccounts = [];
    for (const file of files) {
        try {
            const text = await file.text();
            const data = JSON.parse(text);
            if (Array.isArray(data)) {
                newAccounts.push(...data);
            } else {
                newAccounts.push(data);
            }
        } catch (e) {
            alert(`文件 $${file.name} 解析失败: $${e.message}`);
            event.target.value = '';
            return;
        }
    }

    if (!newAccounts.length) {
        alert('未找到有效账户数据');
        event.target.value = '';
        return;
    }

    try {
        // 获取现有配置
        const configResp = await fetch(`/${window.ADMIN_PATH}/accounts-config`);
        const configData = await handleApiResponse(configResp);
        const existing = configData.accounts || [];

        // 构建ID到索引的映射
        const idToIndex = new Map();
        existing.forEach((acc, idx) => {
            if (acc.id) idToIndex.set(acc.id, idx);
        });

        // 合并：相同ID覆盖，新ID追加
        let added = 0;
        let updated = 0;
        for (const acc of newAccounts) {
            if (!acc.secure_c_ses || !acc.csesidx || !acc.config_id) continue;
            const accId = acc.id || `account_${existing.length + added + 1}`;
            acc.id = accId;

            if (idToIndex.has(accId)) {
                // 覆盖已存在的账户
                existing[idToIndex.get(accId)] = acc;
                updated++;
            } else {
                // 追加新账户
                existing.push(acc);
                idToIndex.set(accId, existing.length - 1);
                added++;
            }
        }

        if (added === 0 && updated === 0) {
            alert('没有有效账户可导入');
            event.target.value = '';
            return;
        }

        // 保存合并后的配置
        const response = await fetch(`/${window.ADMIN_PATH}/accounts-config`, {
            method: 'PUT',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(existing)
        });

        await handleApiResponse(response);
        event.target.value = '';
        refreshPage();
    } catch (error) {
        console.error('导入失败:', error);
        alert('导入失败: ' + error.message);
        event.target.value = '';
    }
}

// 点击模态框外部关闭
document.getElementById('jsonModal').addEventListener('click', function (e) {
    if (e.target === this) {
        closeModal();
    }
});

// ========== 系统设置相关函数 ==========
async function loadSettings() {
    try {
        const response = await fetch(`/${window.ADMIN_PATH}/settings`);
        const settings = await handleApiResponse(response);

        // 基础配置
        document.getElementById('setting-api-key').value = settings.basic?.api_key || '';
        document.getElementById('setting-base-url').value = settings.basic?.base_url || '';
        document.getElementById('setting-proxy').value = settings.basic?.proxy || '';

        // 临时邮箱配置
        document.getElementById('setting-mail-api').value = settings.basic?.mail_api || '';
        document.getElementById('setting-mail-admin-key').value = settings.basic?.mail_admin_key || '';
        document.getElementById('setting-google-mail').value = settings.basic?.google_mail || '';
        document.getElementById('setting-email-domain').value = settings.basic?.email_domain?.join(',') || '';
        document.getElementById('setting-register-number').value = settings.basic?.register_number || 5;
        document.getElementById('setting-yescaptcha-key').value = settings.basic?.yescaptcha_api_key || '';

        // 图片生成配置
        document.getElementById('setting-image-enabled').checked = settings.image_generation?.enabled ?? true;
        const supportedModels = settings.image_generation?.supported_models || [];
        document.querySelectorAll('#setting-image-models input[type="checkbox"]').forEach(cb => {
            cb.checked = supportedModels.includes(cb.value);
        });

        // 重试策略配置
        document.getElementById('setting-max-new-session').value = settings.retry?.max_new_session_tries || 5;
        document.getElementById('setting-max-retries').value = settings.retry?.max_request_retries || 3;
        document.getElementById('setting-max-switch').value = settings.retry?.max_account_switch_tries || 5;
        document.getElementById('setting-failure-threshold').value = settings.retry?.account_failure_threshold || 3;
        document.getElementById('setting-cooldown').value = settings.retry?.rate_limit_cooldown_seconds || 600;
        document.getElementById('setting-cache-ttl').value = settings.retry?.session_cache_ttl_seconds || 3600;

        // 公开展示配置
        document.getElementById('setting-logo-url').value = settings.public_display?.logo_url || '';
        document.getElementById('setting-chat-url').value = settings.public_display?.chat_url || '';
        document.getElementById('setting-session-hours').value = settings.session?.expire_hours || 24;
    } catch (error) {
        console.error('加载设置失败:', error);
        alert('加载设置失败: ' + error.message);
    }
}

async function saveSettings() {
    try {
        // 收集图片生成支持的模型
        const supportedModels = [];
        document.querySelectorAll('#setting-image-models input[type="checkbox"]:checked').forEach(cb => {
            supportedModels.push(cb.value);
        });

        const settings = {
            basic: {
                api_key: document.getElementById('setting-api-key').value,
                base_url: document.getElementById('setting-base-url').value,
                proxy: document.getElementById('setting-proxy').value,
                mail_api: document.getElementById('setting-mail-api').value,
                mail_admin_key: document.getElementById('setting-mail-admin-key').value,
                google_mail: document.getElementById('setting-google-mail').value,
                email_domain: document.getElementById('setting-email-domain').value.split(',').map(d => d.trim()).filter(d => d),
                register_number: parseInt(document.getElementById('setting-register-number').value) || 5,
                yescaptcha_api_key: document.getElementById('setting-yescaptcha-key').value
            },
            image_generation: {
                enabled: document.getElementById('setting-image-enabled').checked,
                supported_models: supportedModels
            },
            retry: {
                max_new_session_tries: parseInt(document.getElementById('setting-max-new-session').value) || 5,
                max_request_retries: parseInt(document.getElementById('setting-max-retries').value) || 3,
                max_account_switch_tries: parseInt(document.getElementById('setting-max-switch').value) || 5,
                account_failure_threshold: parseInt(document.getElementById('setting-failure-threshold').value) || 3,
                rate_limit_cooldown_seconds: parseInt(document.getElementById('setting-cooldown').value) || 600,
                session_cache_ttl_seconds: parseInt(document.getElementById('setting-cache-ttl').value) || 3600
            },
            public_display: {
                logo_url: document.getElementById('setting-logo-url').value,
                chat_url: document.getElementById('setting-chat-url').value
            },
            session: {
                expire_hours: parseInt(document.getElementById('setting-session-hours').value) || 24
            }
        };

        const response = await fetch(`/${window.ADMIN_PATH}/settings`, {
            method: 'PUT',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(settings)
        });

        await handleApiResponse(response);

        // 直接刷新页面，不显示弹窗
        window.location.reload();
    } catch (error) {
        console.error('保存设置失败:', error);
        alert('保存设置失败: ' + error.message);
    }
}

// 页面加载时自动加载设置
document.addEventListener('DOMContentLoaded', function () {
    loadSettings();

    // 检查是否有正在运行的注册任务
    checkRunningRegisterTask();

    // 启动每分钟轮询刷新账户
    startAccountReloadPolling();
});

// ============ 批量注册功能 ============
let registerPollingInterval = null;
let lastSuccessCount = 0; // 记录上次成功数量

// 检查是否有正在运行的注册任务
async function checkRunningRegisterTask() {
    try {
        const response = await fetch(`/${window.ADMIN_PATH}/register/current`);
        const result = await response.json();

        if (result.task && (result.task.status === 'running' || result.task.status === 'pending')) {
            // 有正在运行的任务，恢复显示
            document.getElementById('register-progress').style.display = 'block';
            lastSuccessCount = result.task.success_count || 0;
            startRegisterPolling(result.task.id);
        }
    } catch (error) {
        console.error('检查注册任务失败:', error);
    }
}

// 启动每分钟轮询刷新账户
function startAccountReloadPolling() {
    setInterval(async () => {
        try {
            await fetch(`/${window.ADMIN_PATH}/accounts/reload`, {
                method: 'POST'
            });
            console.log('[自动刷新] 账户列表已更新');
        } catch (error) {
            console.error('自动刷新账户失败:', error);
        }
    }, 60000); // 60秒
}

// 显示注册模态框
function showRegisterModal() {
    // 从配置中加载域名列表
    const emailDomain = document.getElementById('setting-email-domain').value;
    const domains = emailDomain ? emailDomain.split(',').map(d => d.trim()).filter(d => d) : [];

    // 填充下拉框
    const select = document.getElementById('register-domain');
    select.innerHTML = '<option value="">随机选择域名</option>';
    domains.forEach(domain => {
        const option = document.createElement('option');
        option.value = domain;
        option.textContent = domain;
        select.appendChild(option);
    });

    // 设置默认注册数量
    const registerNumber = parseInt(document.getElementById('setting-register-number').value) || 5;
    document.getElementById('register-count').value = registerNumber;

    document.getElementById('registerModal').style.display = 'flex';
}

// 关闭注册模态框
function closeRegisterModal() {
    document.getElementById('registerModal').style.display = 'none';
}

// 开始注册
async function startRegister() {
    const count = parseInt(document.getElementById('register-count').value);
    const domain = document.getElementById('register-domain').value;

    if (!count || count < 1 || count > 50) {
        alert('请输入有效的注册数量（1-50）');
        return;
    }

    try {
        const response = await fetch(`/${window.ADMIN_PATH}/register/start`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({
                count: count,
                domain: domain || null
            })
        });

        const result = await handleApiResponse(response);

        closeRegisterModal();

        // 显示进度条
        document.getElementById('register-progress').style.display = 'block';

        // 开始轮询任务状态
        startRegisterPolling(result.task.id);

    } catch (error) {
        console.error('启动注册失败:', error);
        alert('启动注册失败: ' + error.message);
    }
}

// 开始轮询注册任务状态
function startRegisterPolling(taskId) {
    updateRegisterProgress(taskId);

    registerPollingInterval = setInterval(() => {
        updateRegisterProgress(taskId);
    }, 2000); // 每2秒更新一次
}

async function updateRegisterProgress(taskId) {
    try {
        const response = await fetch(`/${window.ADMIN_PATH}/register/task/${taskId}`);
        const result = await response.json();
        const task = result.task;

        // 更新进度显示
        document.getElementById('reg-current').textContent = task.progress || 0;
        document.getElementById('reg-total').textContent = task.count || 0;
        document.getElementById('reg-success').textContent = task.success_count || 0;
        document.getElementById('reg-fail').textContent = task.fail_count || 0;

        const total = task.count || 1;
        const current = task.progress || 0;
        const percentage = Math.round((current / total) * 100);
        document.getElementById('reg-progress-bar').style.width = percentage + '%';

        const successRate = current > 0 ? Math.round((task.success_count / current) * 100) : 0;
        document.getElementById('reg-rate').textContent = successRate + '%';

        // 检查是否有新的成功注册（每成功一个就刷新）
        const currentSuccessCount = task.success_count || 0;
        if (currentSuccessCount > lastSuccessCount) {
            lastSuccessCount = currentSuccessCount;

            // 刷新账户列表
            try {
                await fetch(`/${window.ADMIN_PATH}/accounts/reload`, {
                    method: 'POST'
                });
                console.log(`[注册] 成功 ${currentSuccessCount} 个，账户列表已刷新`);
            } catch (error) {
                console.error('刷新账户列表失败:', error);
            }
        }

        // 如果任务完成，停止轮询
        if (task.status === 'success' || task.status === 'failed') {
            clearInterval(registerPollingInterval);
            registerPollingInterval = null;
            lastSuccessCount = 0;

            // 最后再刷新一次确保所有账户都加载了
            try {
                await fetch(`/${window.ADMIN_PATH}/accounts/reload`, {
                    method: 'POST'
                });
            } catch (error) {
                console.error('最终刷新账户失败:', error);
            }

            // 3秒后刷新页面
            setTimeout(() => {
                window.location.reload();
            }, 3000);
        }
    } catch (error) {
        console.error('获取注册进度失败:', error);
        clearInterval(registerPollingInterval);
        registerPollingInterval = null;
        lastSuccessCount = 0;
    }
}

// ============ Cookie 刷新功能 ============
let refreshPollingInterval = null;
let lastRefreshSuccessCount = 0;

// 刷新单个账户
async function refreshAccount(accountId) {
    if (!confirm(`确定要刷新账户 ${accountId} 的 Cookie 吗？\n\n注意：刷新过程需要约30秒，请耐心等待。`)) return;

    try {
        const response = await fetch(`/${window.ADMIN_PATH}/login/start`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify([accountId])
        });

        const result = await handleApiResponse(response);

        // 显示进度条
        document.getElementById('refresh-progress').style.display = 'block';

        // 开始轮询任务状态
        startRefreshPolling(result.task.id);

    } catch (error) {
        console.error('刷新账户失败:', error);
        alert('刷新账户失败: ' + error.message);
    }
}

// 刷新全部账户
async function refreshAllAccounts() {
    if (!confirm('确定要刷新所有账户的 Cookie 吗？\n\n注意：每个账户需要约30秒，总时间取决于账户数量。')) return;

    try {
        // 获取所有账户ID
        const accountsResp = await fetch(`/${window.ADMIN_PATH}/accounts`);
        const accountsData = await handleApiResponse(accountsResp);
        const accountIds = accountsData.accounts.map(acc => acc.account_id);

        if (accountIds.length === 0) {
            alert('没有可刷新的账户');
            return;
        }

        const response = await fetch(`/${window.ADMIN_PATH}/login/start`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(accountIds)
        });

        const result = await handleApiResponse(response);

        // 显示进度条
        document.getElementById('refresh-progress').style.display = 'block';

        // 开始轮询任务状态
        startRefreshPolling(result.task.id);

    } catch (error) {
        console.error('批量刷新失败:', error);
        alert('批量刷新失败: ' + error.message);
    }
}

// 开始轮询刷新任务状态
function startRefreshPolling(taskId) {
    updateRefreshProgress(taskId);

    refreshPollingInterval = setInterval(() => {
        updateRefreshProgress(taskId);
    }, 3000); // 每3秒更新一次
}

// 更新刷新进度
async function updateRefreshProgress(taskId) {
    try {
        const response = await fetch(`/${window.ADMIN_PATH}/login/task/${taskId}`);
        const result = await response.json();
        const task = result.task;

        // 更新进度显示
        const refCurrent = document.getElementById('ref-current');
        const refTotal = document.getElementById('ref-total');
        const refSuccess = document.getElementById('ref-success');
        const refFail = document.getElementById('ref-fail');
        const refProgressBar = document.getElementById('ref-progress-bar');

        if (refCurrent) refCurrent.textContent = task.progress || 0;
        if (refTotal) refTotal.textContent = task.account_ids?.length || 0;
        if (refSuccess) refSuccess.textContent = task.success_count || 0;
        if (refFail) refFail.textContent = task.fail_count || 0;

        const total = task.account_ids?.length || 1;
        const current = task.progress || 0;
        const percentage = Math.round((current / total) * 100);
        if (refProgressBar) refProgressBar.style.width = percentage + '%';

        // 检查是否有新的成功刷新
        const currentSuccessCount = task.success_count || 0;
        if (currentSuccessCount > lastRefreshSuccessCount) {
            lastRefreshSuccessCount = currentSuccessCount;

            // 刷新账户列表
            try {
                await fetch(`/${window.ADMIN_PATH}/accounts/reload`, {
                    method: 'POST'
                });
                console.log(`[刷新] 成功 ${currentSuccessCount} 个，账户列表已更新`);
            } catch (error) {
                console.error('刷新账户列表失败:', error);
            }
        }

        // 如果任务完成，停止轮询
        if (task.status === 'success' || task.status === 'failed') {
            clearInterval(refreshPollingInterval);
            refreshPollingInterval = null;
            lastRefreshSuccessCount = 0;

            // 最后再刷新一次确保更新
            try {
                await fetch(`/${window.ADMIN_PATH}/accounts/reload`, {
                    method: 'POST'
                });
            } catch (error) {
                console.error('最终刷新账户失败:', error);
            }

            // 显示结果提示
            const successCount = task.success_count || 0;
            const failCount = task.fail_count || 0;
            console.log(`[刷新完成] 成功: ${successCount}, 失败: ${failCount}`);

            // 3秒后刷新页面
            setTimeout(() => {
                window.location.reload();
            }, 3000);
        }
    } catch (error) {
        console.error('获取刷新进度失败:', error);
        clearInterval(refreshPollingInterval);
        refreshPollingInterval = null;
        lastRefreshSuccessCount = 0;
    }
}


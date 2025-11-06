from celery import Celery
from kombu import Queue
from app.core.config import settings
from app.utils.logger import logger

# ===============================
# 🚀 Celery 实例初始化
# ===============================
celery_app = Celery(
    "bots_eval",
    broker=settings.REDIS_BROKER_URL,
    backend=settings.REDIS_BACKEND_URL,
    include=[
        "app.services.test_tasks",  # ✅ 必须显式导入任务模块
    ],
)
# ===============================
# ⚙️ Celery 配置参数
# ===============================
celery_app.conf.update(
    # 并发控制
    worker_concurrency=settings.CELERY_CONCURRENCY,  # 每个 worker 同时运行任务数量
    worker_prefetch_multiplier=1,                    # 禁止 worker 预取太多任务（防止阻塞）
    task_acks_late=True,                             # 任务执行完再确认（防止丢任务）
    task_reject_on_worker_lost=True,                 # Worker 崩溃后任务重新入队

    # 队列配置
    task_default_queue="default",
    task_queues=(
        Queue("default"),
        Queue("high_priority"),
        Queue("low_priority"),
    ),

    # 可见性超时，任务执行超时重入队列（秒）
    broker_transport_options={"visibility_timeout": 3600},

    # 时区与序列化配置
    timezone=settings.CELERY_TIMEZONE,
    enable_utc=False,
    task_serializer="json",
    result_serializer="json",
    accept_content=["json"],

    # 日志
    worker_log_format="[%(asctime)s: %(levelname)s/%(processName)s] %(message)s",
    worker_task_log_format="[%(asctime)s: %(levelname)s/%(processName)s] [%(task_name)s(%(task_id)s)] %(message)s",
)

# ===============================
# 🧩 健康检测任务
# ===============================
@celery_app.task(name="celery.health_check")
def health_check():
    """Celery 健康检测任务"""
    logger.info("✅ Celery worker 正常运行中")
    return {"status": "ok"}


# ===============================
# ✅ 调试信息
# ===============================
logger.info("✅ Celery 初始化完成")
logger.info(f"Broker: {settings.REDIS_BROKER_URL}")
logger.info(f"Backend: {settings.REDIS_BACKEND_URL}")
logger.info(f"Concurrency: {settings.CELERY_CONCURRENCY}")

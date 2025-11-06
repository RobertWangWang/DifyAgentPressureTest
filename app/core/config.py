import os
from dotenv import load_dotenv
from app.utils.logger import logger

# === 环境变量加载 ===
base_dir = os.path.dirname(os.path.abspath(__file__))
env_path = os.path.join(base_dir, "../config/.env.prod")
logger.info(f"🔧 加载环境变量: {env_path}")
load_dotenv(dotenv_path=env_path, verbose=True)


class Settings:
    """
    全局配置类：数据库 + Redis + Celery
    """

    # === 数据库配置 ===
    db_username = os.getenv("DB_USERNAME")
    db_password = os.getenv("DB_PASSWORD")
    db_host = os.getenv("DB_HOST", "localhost")
    db_port = int(os.getenv("DB_PORT", 3306))
    db_name = os.getenv("DB_DATABASE")

    MYSQL_URL = os.getenv(
        "MYSQL_URL",
        f"mysql+pymysql://{db_username}:{db_password}@{db_host}:{db_port}/{db_name}?charset=utf8mb4",
    )

    ASYNC_MYSQL_URL = os.getenv(
        "ASYNC_MYSQL_URL",
        f"mysql+aiomysql://{db_username}:{db_password}@{db_host}:{db_port}/{db_name}?charset=utf8mb4",
    )

    FILE_UPLOAD_DIR = os.getenv("FILE_UPLOAD_DIR", "uploads")

    # === Redis / Celery 配置 ===
    REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
    REDIS_PORT = os.getenv("REDIS_PORT", "6379")
    REDIS_PASSWORD = os.getenv("REDIS_PASSWORD", "")

    REDIS_BROKER_URL = f"redis://:{REDIS_PASSWORD}@{REDIS_HOST}:{REDIS_PORT}/14"
    REDIS_BACKEND_URL = f"redis://:{REDIS_PASSWORD}@{REDIS_HOST}:{REDIS_PORT}/15"

    CELERY_CONCURRENCY = int(os.getenv("CELERY_CONCURRENCY", 10))
    CELERY_TIMEZONE = os.getenv("CELERY_TIMEZONE", "Asia/Shanghai")

    # === 打印调试信息 ===
    logger.info(f"✅ MySQL URL: {MYSQL_URL}")
    logger.info(f"✅ Async MySQL URL: {ASYNC_MYSQL_URL}")
    logger.info(f"✅ Redis Broker: {REDIS_BROKER_URL}")
    logger.info(f"✅ Redis Backend: {REDIS_BACKEND_URL}")
    logger.info(f"✅ Celery Concurrency: {CELERY_CONCURRENCY}")


# === 初始化配置实例 ===
settings = Settings()

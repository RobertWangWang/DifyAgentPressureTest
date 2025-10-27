import os
from dotenv import load_dotenv
from pathlib import Path

from app.utils.logger import logger

base_dir = os.path.dirname(os.path.abspath(__file__))
env_path = os.path.join(base_dir, ".env")
logger.info(env_path)
load_dotenv(dotenv_path=env_path, verbose=True)


class Settings:
    """
    配置项
    """
    db_username = os.environ.get("DB_USERNAME")
    logger.info(f"db_username: {db_username}")
    db_password = os.getenv("DB_PASSWORD")
    logger.info(f"db_password: {db_password}")
    db_host: str = os.getenv("DB_HOST")
    logger.info(f"db_host: {db_host}")
    db_port: int = int(os.getenv("DB_PORT", 3306))
    logger.info(f"db_port: {db_port}")
    db_name: str = os.getenv("DB_DATABASE")
    logger.info(f"db_name: {db_name}")
    MYSQL_URL: str = os.getenv(
        "MYSQL_URL",
        f"mysql+pymysql://{db_username}:{db_password}@{db_host}:{db_port}/{db_name}?charset=utf8mb4"
    )
    FILE_UPLOAD_DIR: str = os.getenv("FILE_UPLOAD_DIR", "uploads")

settings = Settings()

from loguru import logger
import sys
from pathlib import Path
from collections import deque
import threading

# ---------------------------
# 日志初始化配置
# ---------------------------
LOG_DIR = Path(__file__).resolve().parent.parent / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

LOG_PATH = LOG_DIR / "app.log"

# ---------------------------
# 限制日志行数配置
# ---------------------------
MAX_LOG_LINES = 10_000
_log_lock = threading.Lock()
_log_buffer = deque(maxlen=MAX_LOG_LINES)  # 固定长度队列，超出后自动丢弃旧行

def rotating_sink(message):
    """自定义 sink：记录日志并仅保留最新 MAX_LOG_LINES 行"""
    with _log_lock:
        _log_buffer.append(message)
        # 每写一行都更新文件
        with open(LOG_PATH, "w", encoding="utf-8") as f:
            f.writelines(_log_buffer)

# ---------------------------
# 移除默认配置
# ---------------------------
logger.remove()

# ---------------------------
# 控制台日志（带颜色）
# ---------------------------
logger.add(
    sys.stdout,
    colorize=True,
    format="<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> "
           "| <level>{level: <8}</level> "
           "| <cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> "
           "- <level>{message}</level>",
    level="DEBUG",
)

# ---------------------------
# 文件日志（仅保留最新 10000 行）
# ---------------------------
logger.add(
    rotating_sink,
    enqueue=True,           # ✅ 多线程安全
    backtrace=True,
    diagnose=True,
    level="INFO",
    format="{time:YYYY-MM-DD HH:mm:ss.SSS} | {level: <8} | "
           "{process.name}({process.id}) | {thread.name}({thread.id}) | "
           "{name}:{function}:{line} - {message}",
)

logger.info(f"Logger initialized (max {MAX_LOG_LINES} lines) at: {LOG_PATH}")

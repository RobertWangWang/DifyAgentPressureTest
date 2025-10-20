from loguru import logger
import sys
from pathlib import Path

# ---------------------------
# 日志初始化配置
# ---------------------------
LOG_DIR = Path(__file__).resolve().parent.parent / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

LOG_PATH = LOG_DIR / "app.log"

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
# 文件日志（仅追加）
# ---------------------------
logger.add(
    LOG_PATH,
    mode="a",               # ✅ 明确指定为追加模式
    encoding="utf-8",
    enqueue=True,           # ✅ 多进程安全
    backtrace=True,         # ✅ 捕捉堆栈
    diagnose=True,          # ✅ 展示变量内容
    level="INFO",
    format="{time:YYYY-MM-DD HH:mm:ss.SSS} | {level: <8} | "
           "{process.name}({process.id}) | {thread.name}({thread.id}) | "
           "{name}:{function}:{line} - {message}",
)

logger.info(f"Logger initialized (append mode) at: {LOG_PATH}")

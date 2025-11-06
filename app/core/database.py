from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine

from app.core.config import settings

engine = create_engine(
    settings.MYSQL_URL,
    echo_pool=True,
    echo=True,
    pool_size=128,          # 连接池中保持的连接数（默认 5）
    max_overflow=256,       # 超出 pool_size 后允许的最大连接数（相当于“worker 上限”）
    pool_timeout=30,       # 获取连接的超时时间（秒）
    pool_recycle=180,     # 回收连接前的存活时间（秒，防止MySQL断开）
)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)
async_engine = create_async_engine(
    settings.ASYNC_MYSQL_URL,
    echo=False,
    future=True,
    pool_size=128,           # 默认是 5，建议至少 10~20
    max_overflow=256,        # 默认是 10，建议 2x pool_size
    pool_timeout=60,        # 超时等待连接时间（默认 30 秒）
    pool_recycle=1800,      # 30 分钟自动回收（防止 MySQL 断开）
    pool_pre_ping=True,     # 检查空闲连接是否存活
)

AsyncSessionLocal = sessionmaker(async_engine, expire_on_commit=False, class_=AsyncSession)

# ✅ FastAPI 依赖式 Session
def get_db():
    """
    FastAPI 同步数据库依赖（yield模式）
    """
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# ✅ 可在 Celery 中直接调用的 Session 工具函数
def get_sync_db():
    """
    在 Celery 等非 FastAPI 环境中直接获取同步 Session
    """
    db = SessionLocal()
    return db

class Base(DeclarativeBase):
    pass

def init_db():
    """
    在应用启动时调用此函数，自动创建所有尚不存在的表。
    """
    # 注意：Base.metadata 包含所有通过 Base 映射的模型
    Base.metadata.create_all(engine)
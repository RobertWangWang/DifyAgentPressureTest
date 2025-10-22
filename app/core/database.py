from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import DeclarativeBase

from app.core.config import settings

engine = create_engine(settings.MYSQL_URL, echo=True)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)

class Base(DeclarativeBase):
    pass

def init_db():
    """
    在应用启动时调用此函数，自动创建所有尚不存在的表。
    """
    # 注意：Base.metadata 包含所有通过 Base 映射的模型
    Base.metadata.create_all(engine)
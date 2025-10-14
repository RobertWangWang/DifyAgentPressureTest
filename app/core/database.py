from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from app.core.config import settings
from app.models.test_chatflow_record import Base

engine = create_engine(settings.MYSQL_URL, echo=True)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)

def init_db():
    """
    在应用启动时调用此函数，自动创建所有尚不存在的表。
    """
    # 注意：Base.metadata 包含所有通过 Base 映射的模型
    Base.metadata.create_all(engine)
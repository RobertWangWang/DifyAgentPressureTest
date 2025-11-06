from sqlalchemy import Column, Integer, String, DateTime, Boolean, Text, func
import uuid as uuid_lib
from datetime import datetime, timedelta, timezone
from sqlalchemy.orm import Mapped, mapped_column
beijing_tz = timezone(timedelta(hours=8))

from app.core.database import Base

class PromptTemplate(Base):
    __tablename__ = "bots_eval_prompt_template"
    __table_args__ = {
        "mysql_charset": "utf8mb4",
        "mysql_collate": "utf8mb4_unicode_ci",
        "comment": "模型厂商提供，支持中文模糊搜索"
    }
    # 主键
    record_id = Column(Integer, primary_key=True, autoincrement=True)

    # 全局唯一标识
    uuid = Column(String(36), unique=True, nullable=False, default=lambda: str(uuid_lib.uuid4()))

    # 创建时间，默认当前时间
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        comment="创建时间（北京时间）",
        default=lambda: datetime.now(beijing_tz),
    )

    # 软删除标识
    deleted_at = Column(Boolean, default=False, nullable=False)

    # 模板内容
    content = Column(Text, nullable=False)

    # 模板名称
    prompt_name = Column(String(255), nullable=False)

    def __repr__(self):
        return f"<PromptTemplate(record_id={self.record_id}, uuid={self.uuid}, deleted_at={self.deleted_at})>"

    def to_dict(self):
        """将对象序列化为 Python 字典"""
        return {
            "record_id": self.record_id,
            "uuid": self.uuid,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "deleted_at": self.deleted_at,
            "content": self.content
        }
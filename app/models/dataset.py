import uuid
from datetime import datetime
from typing import Optional
from sqlalchemy import String, DateTime, Boolean, JSON, UniqueConstraint, func
from sqlalchemy.orm import Mapped, mapped_column
from app.core.database import Base


class Dataset(Base):
    """
    数据集表：用于存储上传的文件信息（与 TestRecord 拆分）
    """
    __tablename__ = "datasets"
    __table_args__ = (
        # ✅ 改为 (uploaded_by, file_md5, agent_id) 三元组唯一约束
        # UniqueConstraint("uploaded_by", "file_md5", "agent_id", name="uq_datasets_uploader_md5_agent"),
        {
            "mysql_charset": "utf8mb4",
            "mysql_collate": "utf8mb4_unicode_ci",
            "comment": "上传数据集文件信息表",
        },
    )

    # ✅ UUID 主键
    uuid: Mapped[str] = mapped_column(
        String(36),
        primary_key=True,
        default=lambda: str(uuid.uuid4()),
        unique=True,
        nullable=False,
        comment="数据集唯一 UUID",
    )

    # ✅ 新增字段
    agent_id: Mapped[str] = mapped_column(String(64), nullable=False, comment="所属的 dify agent ID")

    filename: Mapped[str] = mapped_column(String(255), nullable=False, comment="原始文件名")
    file_md5: Mapped[str] = mapped_column(String(64), nullable=False, comment="文件 MD5 值，用于去重")
    file_suffix: Mapped[Optional[str]] = mapped_column(String(16), nullable=True, comment="文件后缀名，如 .csv/.xlsx")

    tos_key: Mapped[str] = mapped_column(String(255), nullable=False, comment="上传至 TOS 的对象 Key")
    tos_url: Mapped[str] = mapped_column(String(512), nullable=False, comment="上传至 TOS 的完整下载 URL")

    preview_rows: Mapped[Optional[dict]] = mapped_column(JSON, nullable=True, comment="文件前 3 行内容 JSON")
    uploaded_by: Mapped[Optional[str]] = mapped_column(String(64), nullable=True, comment="上传者用户名")

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.convert_tz(func.now(), "+00:00", "+08:00"),
        nullable=False,
        comment="创建时间（北京时间）",
        default=datetime.now(),
    )

    is_deleted: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False, comment="是否逻辑删除")

    # ✅ 输出调试信息
    def __repr__(self) -> str:
        return (
            f"<Dataset(uuid={self.uuid}, filename='{self.filename}', md5='{self.file_md5}', "
            f"uploaded_by='{self.uploaded_by}', agent_id='{self.agent_id}')>"
        )

    # ✅ to_dict 方法（与 TestRecord 风格一致）
    def to_dict(self, exclude_none: bool = False) -> dict:
        """将 ORM 对象转换为可序列化的字典"""
        data = {
            "uuid": self.uuid,
            "agent_id": self.agent_id,
            "filename": self.filename,
            "file_md5": self.file_md5,
            "file_suffix": self.file_suffix,
            "tos_key": self.tos_key,
            "tos_url": self.tos_url,
            "preview_rows": self.preview_rows,
            "uploaded_by": self.uploaded_by,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "is_deleted": self.is_deleted,
        }
        if exclude_none:
            return {k: v for k, v in data.items() if v is not None}
        return data

import uuid
from datetime import datetime
from enum import Enum
from typing import Optional

from sqlalchemy import (
    String,
    Enum as SqlEnum,
    DateTime,
    func,
    Text,
    Integer,
    JSON,
    Boolean,
    ForeignKey,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.core.database import Base


class TestStatus(str, Enum):
    INIT = "init"
    RUNNING = "running"
    CANCELLED = "cancelled"
    FAILED = "failed"
    SUCCESS = "success"
    EXPERIMENT = "experiment"


class AgentType(str, Enum):
    CHATFLOW = "chatflow"
    WORKFLOW = "workflow"


class TestRecord(Base):
    __tablename__ = "test_records"
    __table_args__ = {
        "mysql_charset": "utf8mb4",
        "mysql_collate": "utf8mb4_unicode_ci",
        "comment": "评测任务记录表（引用 Dataset 表）",
    }

    # ✅ 主键
    uuid: Mapped[str] = mapped_column(
        String(36),
        primary_key=True,
        default=lambda: str(uuid.uuid4()),
        unique=True,
        nullable=False,
        comment="测试记录唯一 UUID",
    )

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
        comment="创建时间",
    )

    is_deleted: Mapped[bool] = mapped_column(
        Boolean,
        default=False,
        nullable=False,
        comment="是否逻辑删除",
    )

    # ✅ 数据集引用
    dataset_uuid: Mapped[Optional[str]] = mapped_column(
        String(36),
        ForeignKey("datasets.uuid", ondelete="SET NULL"),
        nullable=True,
        comment="关联数据集 UUID（外键）",
    )

    # ✅ 外键关联
    dataset = relationship("Dataset", lazy="joined")

    filename: Mapped[str] = mapped_column(String(255), nullable=False, comment="评测文件名")

    status: Mapped[TestStatus] = mapped_column(
        SqlEnum(TestStatus, name="test_status_enum"),
        nullable=False,
        default=TestStatus.INIT,
        comment="评测任务状态",
    )

    agent_type: Mapped[AgentType] = mapped_column(
        SqlEnum(AgentType, name="agent_type_enum"),
        nullable=False,
        default=AgentType.CHATFLOW,
        comment="智能体类别",
    )

    task_name: Mapped[str] = mapped_column(String(256), nullable=False, comment="评测任务名称", default="")
    agent_name: Mapped[str] = mapped_column(String(256), nullable=True, comment="智能体名称")

    judge_prompt: Mapped[str] = mapped_column(String(2048), nullable=False, comment="评测提示词", default="")
    judge_model: Mapped[Optional[str]] = mapped_column(String(256), nullable=True, comment="评测模型名称")
    judge_model_provider_name: Mapped[Optional[str]] = mapped_column(String(256), nullable=True, comment="模型供应商")

    duration: Mapped[Optional[int]] = mapped_column(Integer, comment="任务耗时")
    result: Mapped[Optional[dict]] = mapped_column(JSON, comment="评测结果")
    concurrency: Mapped[int] = mapped_column(Integer, default=1, comment="并发数")

    # ✅ Dify 相关字段
    dify_api_url: Mapped[str] = mapped_column(String(512), nullable=False, comment="Dify API URL")
    dify_bearer_token: Mapped[str] = mapped_column(String(512), nullable=False, comment="Dify Bearer Token")
    dify_test_agent_id: Mapped[str] = mapped_column(String(256), nullable=False, comment="Dify 测试 Agent ID")
    dify_api_key: Mapped[Optional[str]] = mapped_column(String(256), comment="Dify API Key")
    dify_account_id: Mapped[Optional[str]] = mapped_column(String(64), comment="Dify Account ID")
    dify_username: Mapped[str] = mapped_column(String(256), nullable=False, comment="Dify 用户名")

    success_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False, comment="成功次数")
    failure_count: Mapped[int] = mapped_column(Integer, default=0, nullable=False, comment="失败次数")

    # ✅ 兼容历史字段（不再使用）
    dataset_tos_key: Mapped[Optional[str]] = mapped_column(String(512), comment="TOS Key（兼容历史）")
    dataset_tos_url: Mapped[Optional[str]] = mapped_column(String(1024), comment="TOS URL（兼容历史）")
    dataset_file_md5: Mapped[Optional[str]] = mapped_column(String(64), comment="文件 MD5（兼容历史）")

    def __repr__(self) -> str:
        return f"<TestRecord(uuid={self.uuid}, task={self.task_name}, status={self.status})>"

    def to_dict(self, exclude_none: bool = False, include_dataset: bool = False) -> dict:
        """
        转换为字典
        :param exclude_none: 是否排除 None
        :param include_dataset: 是否包含 dataset 信息
        """
        data = {}
        for column in self.__table__.columns:
            key = column.name
            value = getattr(self, key)
            if isinstance(value, datetime):
                value = value.isoformat()
            if exclude_none and value is None:
                continue
            data[key] = value

        if include_dataset and self.dataset:
            data["dataset"] = self.dataset.to_dict(exclude_none=True)
        return data

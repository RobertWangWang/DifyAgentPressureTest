from sqlalchemy import String, Float, DateTime, JSON, Text, Index, func, Boolean
from sqlalchemy.orm import Mapped, mapped_column
from datetime import datetime
from typing import Optional, Any
import uuid
from app.core.database import Base


class SingleRunResult(Base):
    """
    ORM 映射：单次运行结果表 single_run_result
    """
    __tablename__ = "single_run_result"
    __table_args__ = (
        Index("idx_input_task_uuid", "input_task_uuid"),
        Index("idx_create_time", "create_time"),
        {
            "mysql_charset": "utf8mb4",
            "mysql_collate": "utf8mb4_unicode_ci",
            "comment": "单条运行的测试结果，支持中文模糊搜索"
        }
    )

    # 主键 UUID
    record_id: Mapped[str] = mapped_column(
        String(36),
        primary_key=True,
        default=lambda: str(uuid.uuid4()),
        comment="主键ID（UUID）"
    )

    # chatflow 专用 query
    chatflow_query: Mapped[Optional[str]] = mapped_column(
        String(1024),
        nullable=True,
        default="",
        comment="chatflow专用query"
    )

    # 测评参数 JSON
    test_params: Mapped[Optional[dict[str, Any]]] = mapped_column(
        JSON,
        nullable=True,
        comment="评测参数（JSON）"
    )

    # 输入任务 UUID
    input_task_uuid: Mapped[str] = mapped_column(
        String(64),
        nullable=False,
        comment="输入任务UUID"
    )

    # 评测指标
    input_time_consumption: Mapped[Optional[float]] = mapped_column(Float, comment="耗时（秒）")
    input_score: Mapped[Optional[float]] = mapped_column(Float, comment="得分")
    input_tps: Mapped[Optional[float]] = mapped_column(Float, comment="吞吐量（TPS）")

    # 模型生成结果
    input_generated_answer: Mapped[Optional[str]] = mapped_column(
        Text,
        nullable=True,
        comment="生成的答案"
    )

    # 软删除标志
    is_deleted: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False, comment="软删除标志")

    # 创建时间
    create_time: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
        comment="创建时间（UTC）"
    )

    def __repr__(self) -> str:
        return (
            f"<SingleRunResult(record_id={self.record_id}, "
            f"task_uuid={self.input_task_uuid}, "
            f"time={self.input_time_consumption:.3f if self.input_time_consumption else 'N/A'}s, "
            f"score={self.input_score}, tps={self.input_tps}, "
            f"created={self.create_time})>"
        )

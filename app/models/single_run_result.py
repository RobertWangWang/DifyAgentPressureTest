from sqlalchemy import String, Float, DateTime
from sqlalchemy.orm import Mapped, mapped_column
import uuid
from datetime import datetime

# === 基础类 ===
from app.core.database import Base


# === 模型定义 ===
class SingleRunResult(Base):
    """
    ORM 映射：单次运行结果表 single_run_result
    """
    __tablename__ = "single_run_result"

    # 主键：UUID
    record_id: Mapped[str] = mapped_column(
        String(36),
        primary_key=True,
        default=lambda: str(uuid.uuid4()),
        comment="主键ID（UUID）"
    )

    # 输入任务UUID
    input_task_uuid: Mapped[str] = mapped_column(
        String(64),
        nullable=False,
        comment="输入任务UUID"
    )

    # 输入耗时
    input_time_consumption: Mapped[float] = mapped_column(
        Float,
        nullable=True,
        comment="输入任务耗时（秒）"
    )

    # 输入得分
    input_score: Mapped[float] = mapped_column(
        Float,
        nullable=True,
        comment="输入任务得分"
    )

    # 输入TPS
    input_tps: Mapped[float] = mapped_column(
        Float,
        nullable=True,
        comment="输入任务TPS（吞吐量）"
    )

    # 输入生成答案
    input_generated_answer: Mapped[str] = mapped_column(
        String(2048),
        nullable=True,
        comment="输入任务生成的答案"
    )

    create_time: Mapped[datetime] = mapped_column(
        DateTime,
        default=datetime.utcnow,
        nullable=False,
        comment="创建时间（UTC）"
    )

    def __repr__(self) -> str:
        return (
            f"<SingleRunResult("
            f"record_id={self.record_id}, "
            f"input_task_uuid={self.input_task_uuid}, "
            f"input_time_consumption={self.input_time_consumption}, "
            f"input_score={self.input_score}, "
            f"input_tps={self.input_tps}, "
            f"input_generated_answer={self.input_generated_answer}, "
            f"create_time={self.create_time})>"
        )

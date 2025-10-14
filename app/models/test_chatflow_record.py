import uuid
from datetime import datetime
from enum import Enum

from sqlalchemy import (
    String,
    Enum as SqlEnum,
    DateTime,
    func,
    Text,
    Integer
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class TestStatus(str, Enum):
    INIT = "init"
    RUNNING = "running"
    FAILED = "failed"


class TestRecord(Base):
    __tablename__ = "test_chatflow_records"

    # 用 uuid 作为主键（字符串形式）
    uuid: Mapped[str] = mapped_column(
        String(36),
        primary_key=True,
        default=lambda: str(uuid.uuid4()),
        unique=True,
        nullable=False
    )

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False
    )

    filename: Mapped[str] = mapped_column(String(255), nullable=False)

    status: Mapped[TestStatus] = mapped_column(
        SqlEnum(TestStatus, name="test_status_enum"),
        nullable=False,
        default=TestStatus.INIT
    )

    duration: Mapped[int] = mapped_column(Integer, nullable=True)

    result: Mapped[str] = mapped_column(String(2048), nullable=True)

    concurrency: Mapped[int] = mapped_column(Integer, nullable=True, default=1)

    dify_api_url: Mapped[str] = mapped_column(String(512), nullable=False)

    dify_api_key: Mapped[str] = mapped_column(String(256), nullable=False)

    dify_username: Mapped[str] = mapped_column(String(256), nullable=False)

    chatflow_query: Mapped[str] = mapped_column(Text, nullable=False)

    def __repr__(self) -> str:
        return (
            f"<TestRecord(uuid='{self.uuid}', status='{self.status}', "
            f"duration={self.duration}, file='{self.filename}')>"
        )

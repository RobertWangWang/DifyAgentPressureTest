import uuid
from datetime import datetime
from enum import Enum

from sqlalchemy import (
    String,
    Enum as SqlEnum,
    DateTime,
    func,
    Text,
    Integer,
    JSON
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column


class Base(DeclarativeBase):
    pass


class TestStatus(str, Enum):
    INIT = "init"
    RUNNING = "running"
    FAILED = "failed"
    SUCCESS = "success"

class AgentType(str, Enum):
    CHATFLOW = "chatflow"
    WORKFLOW = "workflow"

class TestRecord(Base):
    __tablename__ = "test_records"

    # 用 uuid 作为主键（字符串形式）
    uuid: Mapped[str] = mapped_column(
        String(36),
        primary_key=True,
        default=lambda: str(uuid.uuid4()),
        unique=True,
        nullable=False,
        comment="数据库主键，字符串格式 UUID"
    )

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
        comment="创建时间"
    )

    filename: Mapped[str] = mapped_column(String(255), nullable=False, comment="评测用到的文件名")

    status: Mapped[TestStatus] = mapped_column(
        SqlEnum(TestStatus, name="test_status_enum"),
        nullable=False,
        default=TestStatus.INIT,
        comment="评测任务当前状态,枚举"
    )

    agent_type: Mapped[AgentType] = mapped_column(
        SqlEnum(AgentType, name="agent_type_enum"),
        nullable=False,
        default="",
        comment="智能体的类别，默认chatflow"
    )

    judge_prompt: Mapped[str] = mapped_column(String(2048), nullable=False, comment="评测任务大模型判断提示词", default="")

    agent_name: Mapped[str] = mapped_column(String(256), nullable=True, comment="智能体名称")

    task_name: Mapped[str] = mapped_column(String(256), nullable=False, comment="评测任务名称", default="")

    duration: Mapped[int] = mapped_column(Integer, nullable=True, comment="评测任务耗时")

    result: Mapped[str] = mapped_column(JSON, nullable=True, comment="评测结果")

    concurrency: Mapped[int] = mapped_column(Integer, nullable=True, default=1, comment="并发数")

    dify_api_url: Mapped[str] = mapped_column(String(512), nullable=False, comment="dify api url")

    dify_bearer_token: Mapped[str] = mapped_column(String(512), nullable=False, comment="dify bearer token")

    dify_test_agent_id: Mapped[str] = mapped_column(String(256), nullable=False, comment="dify test agent id")

    dify_api_key: Mapped[str] = mapped_column(String(256), nullable=True, comment="dify api key,传入bearer token后生成")

    dify_account_id: Mapped[str] = mapped_column(String(64), nullable=True, comment="dify account id")

    success_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0, comment="成功次数")

    failure_count: Mapped[int] = mapped_column(Integer, nullable=False, default=0, comment="失败次数")

    dify_username: Mapped[str] = mapped_column(String(256), nullable=False, comment="评测任务dify用户名")

    chatflow_query: Mapped[str] = mapped_column(Text, nullable=True, comment="chatflow query", default="")

    def __repr__(self) -> str:
        return (
            f"<TestRecord(uuid='{self.uuid}', status='{self.status}', "
            f"duration={self.duration}, file='{self.filename}')>"
        )

    def to_dict(self, exclude_none: bool = False) -> dict:
        """
        将 ORM 对象转换为 Python 字典。
        参数:
            exclude_none: 是否排除值为 None 的字段。
        """
        result = {}
        for column in self.__table__.columns:
            key = column.name
            value = getattr(self, key)
            # 日期类型转字符串
            if isinstance(value, datetime):
                value = value.isoformat()
            # 排除 None
            if exclude_none and value is None:
                continue
            result[key] = value
        return result
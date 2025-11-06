import asyncio
from concurrent.futures import ThreadPoolExecutor
executor = ThreadPoolExecutor(max_workers=128)
asyncio.get_event_loop().set_default_executor(executor)
import uuid
from datetime import datetime, timedelta, timezone
beijing_tz = timezone(timedelta(hours=8))
from enum import Enum
from typing import Optional
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, or_
from sqlalchemy.orm import joinedload
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
import os

from app.core.database import Base
from app.schemas.test_record_schema import TestRecordRead
from app.utils.pressure_test_util import dify_api_url_2_account_profile_url,dify_get_account_id
DIFY_API_URL = os.environ.get("TARGET_DIFY_API_URL")


class TestStatus(str, Enum):
    INIT = "init"
    RUNNING = "running"
    CANCELLED = "cancelled"
    FAILED = "failed"
    SUCCESS = "success"
    EXPERIMENT = "experiment"
    PENDING = "pending"


class AgentType(str, Enum):
    CHATFLOW = "chatflow"
    WORKFLOW = "workflow"
    CHAT = "chat"
    AGENT_CHAT = "agent-chat"
    COMPLETION = "completion"


class TestRecord(Base):
    __tablename__ = "bots_eval_test_records"
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
        nullable=False,
        comment="创建时间（北京时间）",
        default=lambda: datetime.now(beijing_tz),
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
        ForeignKey("bots_eval_datasets.uuid", ondelete="SET NULL"),
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

    @staticmethod
    async def get_records_by_keyword_async(
        session,
        key_word: str,
        page: int,
        page_size: int,
        console_token: str,
    ):
        # ✅ 异步防阻塞网络请求
        account_profile_url = dify_api_url_2_account_profile_url(DIFY_API_URL)
        dify_account_id = await asyncio.to_thread(dify_get_account_id, account_profile_url, console_token)

        base_conditions = [
            TestRecord.is_deleted.is_(False),
            TestRecord.status != TestStatus.EXPERIMENT,
            TestRecord.dify_account_id == dify_account_id,
        ]

        like_pattern = f"%{key_word}%" if key_word else None
        query = select(TestRecord).options(joinedload(TestRecord.dataset)).where(*base_conditions)

        if key_word:
            query = query.where(
                or_(
                    TestRecord.task_name.ilike(like_pattern),
                    TestRecord.agent_name.ilike(like_pattern),
                )
            )

        # ✅ 分页 + 排序
        query = query.order_by(TestRecord.created_at.desc()).offset((page - 1) * page_size).limit(page_size)
        result = await session.execute(query)
        records = result.scalars().all()

        # ✅ 总数统计
        count_stmt = select(func.count()).select_from(TestRecord).where(*base_conditions)
        if key_word:
            count_stmt = count_stmt.where(
                or_(
                    TestRecord.task_name.ilike(like_pattern),
                    TestRecord.agent_name.ilike(like_pattern),
                )
            )
        total = (await session.scalar(count_stmt)) or 0

        # ✅ 异步防阻塞数据转换
        records_data = await asyncio.to_thread(
            lambda: [TestRecordRead.model_validate(r) for r in records]
        )

        return {
            "page": page,
            "page_size": page_size,
            "total": total,
            "records": records_data,
        }


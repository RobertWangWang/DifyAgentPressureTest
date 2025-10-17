from datetime import datetime
from enum import Enum as PyEnum
from pydantic import BaseModel, Field


class TestStatus(str, PyEnum):
    INIT = "init"
    RUNNING = "running"
    FAILED = "failed"


class TestRecordBase(BaseModel):
    """共享字段定义（用于创建、读取、更新继承）"""
    status: TestStatus = TestStatus.INIT
    duration: int | None = None
    result: str | None = Field(None, max_length=2048)
    concurrency: int = 1

    dify_api_url: str = Field(..., max_length=512)
    dify_bearer_token: str = Field(..., max_length=512)
    dify_test_agent_id: str = Field(..., max_length=256)
    dify_api_key: str | None = Field(None, max_length=256)
    dify_username: str = Field(..., max_length=256)
    chatflow_query: str = Field(...)

    class Config:
        from_attributes = True


class TestRecordCreate(TestRecordBase):
    """创建时需要的字段"""
    pass


class TestRecordRead(TestRecordBase):
    """响应读取模型"""
    uuid: str
    created_at: datetime
    filename: str


class TestRecordUpdate(BaseModel):
    """部分更新时的字段（全部可选）"""
    status: TestStatus | None = None
    duration: int | None = None
    result: str | None = Field(None, max_length=2048)
    concurrency: int | None = None

    dify_api_url: str | None = Field(None, max_length=512)
    dify_bearer_token: str | None = Field(None, max_length=512)
    dify_test_agent_id: str | None = Field(None, max_length=256)
    dify_api_key: str | None = Field(None, max_length=256)
    dify_username: str | None = Field(None, max_length=256)
    chatflow_query: str | None = None
    filename: str | None = Field(None, max_length=255)

    class Config:
        from_attributes = True

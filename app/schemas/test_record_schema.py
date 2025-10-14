from datetime import datetime
from enum import Enum as PyEnum
from pydantic import BaseModel, Field


class TestStatus(str, PyEnum):
    init = "init"
    running = "running"
    failed = "failed"


class TestRecordCreate(BaseModel):
    filename: str = Field(..., max_length=255)
    status: TestStatus = TestStatus.init
    duration: int | None = None
    result: str | None = Field(None, max_length=2048)
    concurrency: int | None = 1
    dify_api_url: str = Field(..., max_length=512)
    dify_api_key: str = Field(..., max_length=256)
    dify_username: str = Field(..., max_length=256)
    chatflow_query: str = Field(...)

    class Config:
        from_attributes = True


class TestRecordRead(BaseModel):
    uuid: str
    created_at: datetime
    filename: str
    status: TestStatus
    duration: int | None
    result: str | None
    concurrency: int | None
    dify_api_url: str
    dify_api_key: str
    dify_username: str
    chatflow_query: str

    class Config:
        from_attributes = True


class TestRecordUpdate(BaseModel):
    filename: str | None = Field(None, max_length=255)
    status: TestStatus | None = None
    duration: int | None = None
    result: str | None = Field(None, max_length=2048)
    concurrency: int | None = None
    dify_api_url: str | None = Field(None, max_length=512)
    dify_api_key: str | None = Field(None, max_length=256)
    dify_username: str | None = Field(None, max_length=256)
    chatflow_query: str | None = None

    class Config:
        from_attributes = True

from datetime import datetime
from enum import Enum as PyEnum
from typing import Optional, Dict, Any, List
from pydantic import BaseModel, Field


class TestStatus(str, PyEnum):
    INIT = "init"
    RUNNING = "running"
    FAILED = "failed"
    SUCCESS = "success"


class TestRecordBase(BaseModel):
    """共享字段定义（用于创建、读取、更新继承）"""
    status: TestStatus = TestStatus.INIT
    duration: Optional[int] = None
    result: Optional[Dict[str, Any]] = None   # ✅ 改为标准字典类型
    concurrency: int = 1
    task_name: str = Field(..., max_length=256)
    agent_type: str = Field(None, max_length=32)
    agent_name: str = Field(None, max_length=256)
    judge_prompt: str = Field(..., max_length=2048)

    dify_account_id: str = Field(None, max_length=64)
    dify_api_url: str = Field(..., max_length=512)
    dify_bearer_token: str = Field(..., max_length=512)
    dify_test_agent_id: str = Field(..., max_length=256)
    dify_api_key: Optional[str] = Field(None, max_length=256)
    dify_username: str = Field(..., max_length=256)
    chatflow_query: str = Field(None, max_length=1024)

    class Config:
        from_attributes = True
        json_schema_extra = {
            "example": {
                "status": "init",
                "duration": None,
                "result": {"avg_score": 98.5, "avg_time": 9.23},
                "concurrency": 5,
                "dify_api_url": "http://example.com/api",
                "dify_bearer_token": "Bearer xxx",
                "dify_test_agent_id": "agent_123",
                "dify_api_key": "api_key_xxx",
                "dify_username": "robert",
                "chatflow_query": "How are you?",
            }
        }


class TestRecordCreate(TestRecordBase):
    """创建时需要的字段"""
    pass


class TestRecordRead(TestRecordBase):
    """响应读取模型"""
    uuid: str
    created_at: datetime
    filename: str
    task_name: str
    agent_name: str
    agent_type: str
    success_count: int = Field(0, description="成功次数")
    failure_count: int = Field(0, description="失败次数")

class TestRecordUpdate(BaseModel):
    """部分更新时的字段（全部可选）"""
    status: Optional[TestStatus] = None
    duration: Optional[int] = None
    result: Optional[Dict[str, Any]] = None  # ✅ 与 Base 保持一致
    concurrency: Optional[int] = None

    dify_api_url: Optional[str] = Field(None, max_length=512)
    dify_bearer_token: Optional[str] = Field(None, max_length=512)
    dify_test_agent_id: Optional[str] = Field(None, max_length=256)
    dify_api_key: Optional[str] = Field(None, max_length=256)
    dify_username: Optional[str] = Field(None, max_length=256)
    chatflow_query: Optional[str] = None
    filename: Optional[str] = Field(None, max_length=255)

    class Config:
        from_attributes = True

class PaginatedTestRecordResponse(BaseModel):
    page: int
    page_size: int
    total: int
    records: List[TestRecordRead]

class TestRecordsByUUIDAndBearerToken(BaseModel):
    """根据 UUID 和 Bearer Token 获取测试记录"""
    agent_id: str
    bearer_token: str
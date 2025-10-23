from datetime import datetime
from enum import Enum as PyEnum
from typing import Optional, Dict, Any, List
from pydantic import BaseModel, Field


class TestStatus(str, PyEnum):
    INIT = "init"
    RUNNING = "running"
    FAILED = "failed"
    SUCCESS = "success"
    EXPERIMENT = "experiment"


class TestRecordBase(BaseModel):
    """共享字段定义（用于创建、读取、更新继承）"""
    status: TestStatus = TestStatus.INIT
    duration: Optional[int] = None
    result: Optional[Dict[str, Any]] = None
    concurrency: int = 1
    task_name: str = Field(..., max_length=256)
    agent_type: str = Field(None, max_length=32)
    agent_name: str = Field(None, max_length=256)
    judge_prompt: str = Field(..., max_length=2048)
    judge_model: str = Field(..., max_length=256)
    judge_model_provider_name: str = Field(..., max_length=256)

    dify_account_id: str = Field(None, max_length=64)
    dify_api_url: str = Field(..., max_length=512)
    dify_bearer_token: str = Field(..., max_length=512)
    dify_test_agent_id: str = Field(..., max_length=256)
    dify_api_key: Optional[str] = Field(None, max_length=256)
    dify_username: str = Field(..., max_length=256)
    chatflow_query: str = Field(None, max_length=1024)

    # ✅ 新增字段：数据集绝对路径
    dataset_absolute_path: Optional[str] = Field(
        None,
        max_length=1024,
        description="数据集在服务器上的绝对路径"
    )

    # ✅ 新增字段：软删除标记
    is_deleted: bool = Field(
        False,
        description="软删除标记，True 表示已删除"
    )

    class Config:
        from_attributes = True
        json_schema_extra = {
            "example": {
                "status": "init",
                "duration": None,
                "result": {"avg_score": 98.5, "avg_time": 9.23},
                "concurrency": 5,
                "task_name": "EvalTest",
                "agent_type": "chatflow",
                "agent_name": "example_agent",
                "judge_prompt": "Please evaluate response correctness",
                "judge_model": "gpt-4",
                "judge_model_provider_name": "openai",
                "dify_api_url": "http://example.com/api",
                "dify_bearer_token": "Bearer xxx",
                "dify_test_agent_id": "agent_123",
                "dify_api_key": "api_key_xxx",
                "dify_username": "robert",
                "chatflow_query": "How are you?",
                "dataset_absolute_path": "/home/ubuntu/uploads/data.csv",
                "is_deleted": False
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

    dataset_absolute_path: Optional[str] = Field(
        None, description="数据集在服务器上的绝对路径"
    )

    # ✅ 在返回时包含 is_deleted
    is_deleted: bool = Field(
        False, description="软删除标记，True 表示已删除"
    )


class TestRecordUpdate(BaseModel):
    """部分更新时的字段（全部可选）"""
    status: Optional[TestStatus] = None
    duration: Optional[int] = None
    result: Optional[Dict[str, Any]] = None
    concurrency: Optional[int] = None

    dify_api_url: Optional[str] = Field(None, max_length=512)
    dify_bearer_token: Optional[str] = Field(None, max_length=512)
    dify_test_agent_id: Optional[str] = Field(None, max_length=256)
    dify_api_key: Optional[str] = Field(None, max_length=256)
    dify_username: Optional[str] = Field(None, max_length=256)
    chatflow_query: Optional[str] = None
    filename: Optional[str] = Field(None, max_length=255)

    dataset_absolute_path: Optional[str] = Field(
        None, max_length=1024, description="数据集在服务器上的绝对路径"
    )

    # ✅ 更新时也可以设置软删除状态
    is_deleted: Optional[bool] = Field(
        None, description="软删除标记，可用于逻辑删除/恢复"
    )

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


class ExperimentRequest(BaseModel):
    """实验任务请求参数"""
    task_uuid: str


class ExperimentResult(BaseModel):
    """实验结果"""
    time_consumption: float
    token_num: int
    TPS: float
    score: float

class TestRecordStatus(BaseModel):
    """测试记录的当前状态"""

    uuid: str
    status: TestStatus
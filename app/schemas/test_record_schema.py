from datetime import datetime
from enum import Enum as PyEnum
from typing import Optional, Dict, Any, List
from pydantic import BaseModel, Field, ConfigDict

from app.schemas.dataset_schema import DatasetRead  # ✅ 新增导入


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
    agent_type: str = Field(..., max_length=32)
    agent_name: Optional[str] = Field(None, max_length=256)

    judge_prompt: str = Field(..., max_length=2048)
    judge_model: str = Field(..., max_length=256)
    judge_model_provider_name: str = Field(..., max_length=256)

    dify_account_id: Optional[str] = Field(None, max_length=64)
    dify_api_url: str = Field(..., max_length=512)
    dify_bearer_token: str = Field(..., max_length=512)
    dify_test_agent_id: str = Field(..., max_length=256)
    dify_api_key: Optional[str] = Field(None, max_length=256)
    dify_username: str = Field(..., max_length=256)

    # ✅ 新增字段：外键 dataset_uuid
    dataset_uuid: Optional[str] = Field(
        None,
        description="关联数据集 UUID（外键）"
    )

    # ✅ 数据集绝对路径（兼容旧逻辑）
    dataset_absolute_path: Optional[str] = Field(
        None,
        max_length=1024,
        description="数据集在服务器上的绝对路径（历史字段）"
    )

    # ✅ 软删除标记
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
                "dataset_uuid": "8b43b5c7-6c90-48ac-a6a0-0e1e16c5cb9e",
                "dataset_absolute_path": "/home/ubuntu/uploads/data.csv",
                "is_deleted": False
            }
        }


class TestRecordCreate(BaseModel):
    """创建评测任务时的输入模型"""

    task_name: str
    judge_prompt: str
    judge_model: str
    judge_model_provider_name: str

    dify_api_url: str
    dify_test_agent_id: str
    dify_bearer_token: str
    dify_username: str
    dify_api_key: Optional[str] = None   # ✅ 新增字段

    concurrency: int = 1
    dataset_uuid: Optional[str] = None
    dataset_file_md5: Optional[str] = None

    # ✅ 这两个字段由后端补全
    agent_type: Optional[str] = None
    agent_name: Optional[str] = None

    class Config:
        from_attributes = True


class TestRecordRead(BaseModel):
    uuid: str
    task_name: str
    agent_name: str
    agent_type: str
    status: str
    created_at: datetime
    dify_username: str
    concurrency: int
    is_deleted: bool
    judge_model: Optional[str] = None
    judge_model_provider_name: Optional[str] = None
    dataset_file_md5: Optional[str] = None
    dataset_tos_url: Optional[str] = None

    # ✅ 嵌套对象
    dataset: Optional[DatasetRead] = None

    model_config = ConfigDict(from_attributes=True)


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
    filename: Optional[str] = Field(None, max_length=255)
    dataset_uuid: Optional[str] = Field(None, description="关联数据集 UUID")
    dataset_absolute_path: Optional[str] = Field(
        None, max_length=1024, description="数据集绝对路径（历史字段）"
    )
    is_deleted: Optional[bool] = Field(
        None, description="软删除标记，可用于逻辑删除/恢复"
    )

    class Config:
        from_attributes = True


class PaginatedTestRecordResponse(BaseModel):
    """分页返回结构"""
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
    """测试记录的当前状态（带数据集信息）"""

    uuid: str
    status: TestStatus
    task_name: Optional[str] = None
    agent_name: Optional[str] = None
    is_deleted: bool = False
    dataset: Optional[DatasetRead] = None  # ✅ 新增字段

    class Config:
        from_attributes = True


class AgentParameterRequest(BaseModel):
    """代理参数请求参数"""
    agent_id: str
    dify_api_url: str
    bearer_token: str

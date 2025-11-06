from pydantic import BaseModel, Field
from typing import Optional, Any, Dict
from datetime import datetime


# === 基类 ===
class SingleRunResultBase(BaseModel):
    """公共字段（创建/读取通用）"""
    input_task_uuid: str = Field(..., description="输入任务 UUID")
    chatflow_query: Optional[str] = Field(None, description="chatflow 专用 query")
    test_params: Optional[Dict[str, Any]] = Field(None, description="评测参数（JSON）")

    input_time_consumption: Optional[float] = Field(None, description="输入任务耗时（秒）")
    input_score: Optional[float] = Field(None, description="输入任务得分")
    input_tps: Optional[float] = Field(None, description="输入任务 TPS（吞吐量）")
    input_generated_answer: Optional[str] = Field(None, description="输入任务生成的答案")

    is_deleted: bool = Field(default=False, description="软删除标志")

    class Config:
        from_attributes = True  # ✅ 允许 ORM 自动映射
        json_schema_extra = {
            "example": {
                "input_task_uuid": "3d9c3db2-b3f0-4d0a-94ef-ec0c6cc6e123",
                "chatflow_query": "请分析以下文本的情感倾向",
                "test_params": {"temperature": 0.7, "max_tokens": 512},
                "input_time_consumption": 2.384,
                "input_score": 0.92,
                "input_tps": 0.421,
                "input_generated_answer": "文本情感倾向为积极。",
                "is_deleted": False,
            }
        }


# === 创建时使用 ===
class SingleRunResultCreate(SingleRunResultBase):
    """用于创建 SingleRunResult 记录（不含主键与创建时间）"""
    pass


# === 查询 / 返回时使用 ===
class SingleRunResultRead(SingleRunResultBase):
    """用于返回完整记录"""
    record_id: str = Field(..., description="主键 UUID")
    create_time: datetime = Field(..., description="创建时间（UTC）")

    class Config:
        from_attributes = True  # ✅ 支持 ORM -> Pydantic 转换
        json_schema_extra = {
            "example": {
                "record_id": "e5a6d0e8-9d9e-4a41-9d69-9ab7b662a81b",
                "input_task_uuid": "3d9c3db2-b3f0-4d0a-94ef-ec0c6cc6e123",
                "chatflow_query": "请分析以下文本的情感倾向",
                "test_params": {"temperature": 0.7, "max_tokens": 512},
                "input_time_consumption": 2.384,
                "input_score": 0.92,
                "input_tps": 0.421,
                "input_generated_answer": "文本情感倾向为积极。",
                "is_deleted": False,
                "create_time": "2025-10-23T09:30:15.123Z"
            }
        }

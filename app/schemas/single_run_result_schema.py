from pydantic import BaseModel, Field
from typing import Optional
from datetime import datetime


# === 基类 ===
class SingleRunResultBase(BaseModel):
    input_task_uuid: str = Field(..., description="输入任务 UUID")
    input_time_consumption: Optional[float] = Field(None, description="输入任务耗时（秒）")
    input_score: Optional[float] = Field(None, description="输入任务得分")
    input_tps: Optional[float] = Field(None, description="输入任务 TPS（吞吐量）")
    input_generated_answer: Optional[str] = Field(None, description="输入任务生成的答案")

    class Config:
        from_attributes = True  # ✅ 允许从 ORM 对象自动转换

# === 创建时使用 ===
class SingleRunResultCreate(SingleRunResultBase):
    """用于创建 SingleRunResult 记录（无主键）"""
    pass


# === 查询 / 返回时使用 ===
class SingleRunResultRead(SingleRunResultBase):
    """用于返回完整记录"""
    record_id: str = Field(..., description="主键 UUID")
    create_time: datetime = Field(..., description="创建时间")

    class Config:
        from_attributes = True  # ✅ 允许从 ORM 对象自动转换

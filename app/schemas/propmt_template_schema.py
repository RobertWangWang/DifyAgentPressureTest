from datetime import datetime
from pydantic import BaseModel, Field
from typing import Optional

class PromptTemplateBase(BaseModel):
    """公共字段（用于复用）"""
    content: str = Field(..., description="Prompt 模板内容")
    deleted_at: bool = Field(default=False, description="软删除标识")


class PromptTemplateCreate(PromptTemplateBase):
    """用于创建新 PromptTemplate 的 Schema"""
    pass


class PromptTemplateRead(PromptTemplateBase):
    """用于读取 PromptTemplate 的 Schema"""
    record_id: int = Field(..., description="数据库主键 ID")
    uuid: str = Field(..., description="全局唯一标识 UUID")
    created_at: datetime = Field(..., description="创建时间")

    class Config:
        from_attributes = True  # ✅ 支持 SQLAlchemy ORM 对象直接转换（Pydantic v2 新语法）

from datetime import datetime
from typing import Optional, Dict, Any
from pydantic import BaseModel, Field


# === 公共基础模型 ===
class ProviderModelBase(BaseModel):
    """ProviderModel 基础字段（供 Create/Update 共用）"""

    provider_name: str = Field(..., description="服务商名称，如 aliyun_bailian / open_ai / volcengine")
    model_name: str = Field(..., description="模型名称，如 deepseek-v3.1")
    model_type: Optional[str] = Field(None, description="模型类型，如 text-generation / embeddings")

    config: Dict[str, Any] = Field(..., description="模型配置(JSON): 包含 endpointId, apiKey 等信息")

    is_valid: bool = Field(default=True, description="是否有效")
    is_default: bool = Field(default=False, description="是否默认模型")

    account_name: Optional[str] = Field(None, description="账户或分组名称")
    provider_id: Optional[int] = Field(None, description="服务商唯一ID")

    create_by: Optional[str] = Field(None, description="创建者")
    update_by: Optional[str] = Field(None, description="更新者")

    capability: Optional[str] = Field(None, description="模型能力，如 chat, embedding, vision")
    max_token: Optional[int] = Field(None, description="模型最大 token 限制")
    context_length: Optional[int] = Field(None, description="上下文长度限制")

    class Config:
        extra = "ignore"
        json_schema_extra = {
            "example": {
                "provider_name": "aliyun_bailian",
                "model_name": "deepseek-v3.1",
                "model_type": "text-generation",
                "config": {
                    "endpointId": "https://dashscope.aliyuncs.com/api/v1/services/aigc/text-generation/generation",
                    "apiKey": "sk-xxxxxx"
                },
                "is_valid": True,
                "is_default": False,
                "capability": "chat",
                "max_token": 4096,
                "context_length": 8192,
                "account_name": "阿里云百炼账号",
                "provider_id": 1,
                "create_by": "system"
            }
        }


# === 创建模型 ===
class ProviderModelCreate(ProviderModelBase):
    """用于创建 ProviderModel 记录"""
    pass


# === 更新模型 ===
class ProviderModelUpdate(BaseModel):
    """用于部分更新 ProviderModel（PATCH 操作）"""
    provider_name: Optional[str] = None
    model_name: Optional[str] = None
    model_type: Optional[str] = None
    config: Optional[Dict[str, Any]] = None
    is_valid: Optional[bool] = None
    is_default: Optional[bool] = None
    account_name: Optional[str] = None
    provider_id: Optional[int] = None
    update_by: Optional[str] = None
    capability: Optional[str] = None
    max_token: Optional[int] = None
    context_length: Optional[int] = None

    class Config:
        extra = "ignore"


# === 输出模型 ===
class ProviderModelRead(ProviderModelBase):
    """用于数据库读取 / API 响应"""
    id: int = Field(..., description="主键ID")
    create_time: datetime = Field(..., description="创建时间")
    update_time: datetime = Field(..., description="更新时间")

    class Config:

        from_attributes = True  # ✅ SQLAlchemy 2.0 兼容 (代替 orm_mode)

# === 定义请求体 ===
class ProviderQueryRequest(BaseModel):
    provider_name: str
    model_name: str
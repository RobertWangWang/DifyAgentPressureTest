from datetime import datetime
from typing import Optional, List, Dict, Any
from pydantic import BaseModel, Field, ConfigDict


# ✅ 基础模型（供其它 Schema 继承）
class DatasetBase(BaseModel):
    filename: str = Field(..., description="原始文件名")
    file_md5: str = Field(..., description="文件 MD5 值，用于去重")
    file_suffix: Optional[str] = Field(None, description="文件后缀名，如 .csv/.xlsx")
    tos_key: str = Field(..., description="上传至 TOS 的对象 Key")
    tos_url: str = Field(..., description="上传至 TOS 的完整下载 URL")
    preview_rows: Optional[List[Dict[str, Any]]] = Field(None, description="文件前 3 行内容 JSON")
    uploaded_by: Optional[str] = Field(None, description="上传者用户名")


# ✅ 创建时使用（POST /datasets/upload）
class DatasetCreate(BaseModel):
    filename: str = Field(..., description="原始文件名")
    file_md5: str = Field(..., description="文件 MD5 值")
    file_suffix: Optional[str] = Field(None, description="文件后缀名")
    tos_key: str = Field(..., description="TOS 对象 Key")
    tos_url: str = Field(..., description="TOS 下载 URL")
    preview_rows: Optional[List[Dict[str, Any]]] = Field(None, description="文件前 3 行内容")
    uploaded_by: Optional[str] = Field(None, description="上传者用户名")


# ✅ 响应模型（用于返回）
class DatasetRead(BaseModel):
    uuid: str
    filename: str
    file_md5: str
    file_suffix: Optional[str] = None
    tos_key: Optional[str] = None
    tos_url: Optional[str] = None
    created_at: datetime

    model_config = ConfigDict(from_attributes=True)


# ✅ 列表响应（多条）
class DatasetListResponse(BaseModel):
    total: int = Field(..., description="总数量")
    items: list[DatasetRead] = Field(..., description="数据集列表")

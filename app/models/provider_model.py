from datetime import datetime
from sqlalchemy import (
    Column,
    Integer,
    String,
    Boolean,
    DateTime,
    JSON,
)
from app.core.database import Base


class ProviderModel(Base):
    """
    ORM 映射：三方大模型配置表
    来源：provider_models.tsv
    """
    __tablename__ = "provider_models"

    # === 基本信息 ===
    id = Column(Integer, primary_key=True, autoincrement=True, comment="主键ID")
    provider_name = Column(String(128), nullable=False, comment="服务商名称，如 aliyun_bailian / open_ai / volcengine")
    model_name = Column(String(256), nullable=False, comment="模型名称，如 deepseek-v3.1")
    model_type = Column(String(128), nullable=True, comment="模型类型，如 text-generation / embeddings")

    # === 配置 & 状态 ===
    config = Column(JSON, nullable=False, comment="模型配置(JSON): 包含 endpointId, apiKey 等信息")
    is_valid = Column(Boolean, default=True, nullable=False, comment="是否有效")
    is_default = Column(Boolean, default=False, nullable=False, comment="是否默认模型")

    # === 元数据 ===
    create_time = Column(DateTime, default=datetime.utcnow, nullable=False, comment="创建时间")
    update_time = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False, comment="更新时间")

    # === 账号/操作人 ===
    account_name = Column(String(128), nullable=True, comment="账户或分组名称")
    provider_id = Column(String(128), nullable=True, comment="服务商唯一ID")
    create_by = Column(String(128), nullable=True, comment="创建者")
    update_by = Column(String(128), nullable=True, comment="更新者")

    # === 模型能力参数 ===
    capability = Column(String(128), nullable=True, comment="模型能力，如 chat, embedding, vision")
    max_token = Column(Integer, nullable=True, comment="模型最大 token 限制")
    context_length = Column(Integer, nullable=True, comment="上下文长度限制")

    def __repr__(self):
        return f"<ProviderModel(id={self.id}, provider={self.provider_name}, model={self.model_name})>"

# === ✅ 通用 to_dict 方法 ===
    def to_dict(self, exclude_none: bool = False) -> dict:
        """
        将 ORM 对象转换为 Python 字典。
        参数:
            exclude_none: 是否排除值为 None 的字段。
        """
        result = {}
        for column in self.__table__.columns:
            key = column.name
            value = getattr(self, key)
            # 日期类型转字符串
            if isinstance(value, datetime):
                value = value.isoformat()
            # 排除 None
            if exclude_none and value is None:
                continue
            result[key] = value
        return result

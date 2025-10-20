"""
provider_model_crud.py
基于 SQLAlchemy 2.0 ORM 的 ProviderModel CRUD 操作封装
"""

from datetime import datetime
from typing import List, Optional
from sqlalchemy import select, update, delete
from sqlalchemy.orm import Session

from app.models.provider_model import ProviderModel  # 假设上面的定义保存在 models.py


# === CREATE ===
def create_provider_model(
    db: Session,
    provider_name: str,
    model_name: str,
    model_type: Optional[str] = None,
    config: Optional[dict] = None,
    account_name: Optional[str] = None,
    provider_id: Optional[str] = None,
    create_by: Optional[str] = "system",
    capability: Optional[str] = None,
    max_token: Optional[int] = None,
    context_length: Optional[int] = None,
    is_default: bool = False,
    is_valid: bool = True,
) -> ProviderModel:
    """新增一条模型记录"""
    model = ProviderModel(
        provider_name=provider_name,
        model_name=model_name,
        model_type=model_type,
        config=config or {},
        account_name=account_name,
        provider_id=provider_id,
        create_by=create_by,
        update_by=create_by,
        capability=capability,
        max_token=max_token,
        context_length=context_length,
        is_default=is_default,
        is_valid=is_valid,
        create_time=datetime.utcnow(),
        update_time=datetime.utcnow(),
    )
    db.add(model)
    db.commit()
    db.refresh(model)
    return model


# === READ ===
def get_provider_model(db: Session, model_id: int) -> Optional[ProviderModel]:
    """根据 ID 获取单个模型"""
    return db.get(ProviderModel, model_id)


def get_provider_models_by_name(
    db: Session, provider_name: str, model_name: str
) -> list[ProviderModel]:
    """根据服务商与模型名查询（返回所有匹配项）"""
    stmt = select(ProviderModel).where(
        ProviderModel.provider_name == provider_name,
        ProviderModel.model_name == model_name
    )
    return list(db.scalars(stmt).all())



def list_provider_models(
    db: Session,
    provider_name: Optional[str] = None,
    only_valid: bool = True,
    limit: int = 100,
) -> List[ProviderModel]:
    """列出模型列表"""
    stmt = select(ProviderModel)
    if provider_name:
        stmt = stmt.where(ProviderModel.provider_name == provider_name)
    if only_valid:
        stmt = stmt.where(ProviderModel.is_valid.is_(True))
    stmt = stmt.limit(limit)
    return list(db.scalars(stmt))


# === UPDATE ===
def update_provider_model(
    db: Session,
    model_id: int,
    update_data: dict,
    update_by: str = "admin",
) -> Optional[ProviderModel]:
    """更新模型配置"""
    stmt = (
        update(ProviderModel)
        .where(ProviderModel.id == model_id)
        .values(**update_data, update_time=datetime.utcnow(), update_by=update_by)
        .returning(ProviderModel)
    )
    result = db.execute(stmt)
    db.commit()
    return result.scalar_one_or_none()


def toggle_model_validity(db: Session, model_id: int, is_valid: bool):
    """启用/禁用模型"""
    return update_provider_model(db, model_id, {"is_valid": is_valid})


def set_default_model(db: Session, provider_name: str, model_name: str):
    """将指定模型设为默认（并取消同 provider 的其他默认标志）"""
    db.execute(
        update(ProviderModel)
        .where(ProviderModel.provider_name == provider_name)
        .values(is_default=False)
    )
    db.execute(
        update(ProviderModel)
        .where(
            ProviderModel.provider_name == provider_name,
            ProviderModel.model_name == model_name,
        )
        .values(is_default=True, update_time=datetime.utcnow())
    )
    db.commit()


# === DELETE ===
def delete_provider_model(db: Session, model_id: int):
    """删除指定模型"""
    db.execute(delete(ProviderModel).where(ProviderModel.id == model_id))
    db.commit()

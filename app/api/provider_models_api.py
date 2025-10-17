from datetime import datetime
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session

from app.core.database import SessionLocal
from app.models.provider_model import ProviderModel
from app.schemas.provider_model_schema import (
    ProviderModelCreate,
    ProviderModelRead,
    ProviderModelUpdate,
    ProviderQueryRequest
)
from app.services.provider_model_services import llm_connection_test

# === 数据库依赖 ===
def get_db():
    """FastAPI 依赖注入：自动创建并关闭 Session"""
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


# === 路由定义 ===
router = APIRouter(
    prefix="/provider_models",
    tags=["Provider Models"],
)


# === CREATE ===
@router.post("/", response_model=ProviderModelRead, status_code=status.HTTP_201_CREATED)
def create_provider_model(
    data: ProviderModelCreate,
    db: Session = Depends(get_db),
):
    """创建新的 ProviderModel 记录"""
    model = ProviderModel(**data.dict())
    db.add(model)
    db.commit()
    db.refresh(model)
    return model


# === READ ALL ===
@router.get("/", response_model=List[ProviderModelRead])
def list_provider_models(
    db: Session = Depends(get_db),
    provider_name: Optional[str] = None,
    is_valid: Optional[bool] = True,
    limit: int = 100,
):
    """获取模型列表，可选按服务商过滤"""
    query = db.query(ProviderModel)
    if provider_name:
        query = query.filter(ProviderModel.provider_name == provider_name)
    if is_valid is not None:
        query = query.filter(ProviderModel.is_valid == is_valid)
    return query.limit(limit).all()


# === READ SINGLE ===
@router.get("/{model_id:int}", response_model=ProviderModelRead)
def get_provider_model(model_id: int, db: Session = Depends(get_db)):
    """根据 ID 获取模型"""
    model = db.get(ProviderModel, model_id)
    print(model_id,model_id,model_id)
    if not model:
        raise HTTPException(status_code=404, detail="Model not found")
    return model


# === READ BY NAME ===

# === POST 查询接口 ===
@router.post("/query", response_model=List[ProviderModelRead])
def query_provider_models(
    body: ProviderQueryRequest,
    db: Session = Depends(get_db),
):
    """
    根据 provider_name 与 model_name 同时查询匹配的模型
    示例请求:
    {
      "provider_name": "volcengine",
      "model_name": "DeepSeek-V3"
    }
    """
    models = (
        db.query(ProviderModel)
        .filter(
            ProviderModel.provider_name == body.provider_name,
            ProviderModel.model_name == body.model_name,
        )
        .all()
    )

    llm_connection_test(candidate_models= models)
    # 不抛 404，返回空列表更友好
    return models


# === UPDATE ===
@router.put("/{model_id:int}", response_model=ProviderModelRead)
def update_provider_model(
    model_id: int,
    update_data: ProviderModelUpdate,
    db: Session = Depends(get_db),
):
    """更新模型配置"""
    model = db.get(ProviderModel, model_id)
    if not model:
        raise HTTPException(status_code=404, detail="Model not found")

    for field, value in update_data.dict(exclude_unset=True).items():
        setattr(model, field, value)

    model.update_time = datetime.utcnow()  # ✅ 自动更新时间

    db.commit()
    db.refresh(model)
    return model


# === DELETE ===
@router.delete("/{model_id:int}", status_code=status.HTTP_204_NO_CONTENT)
def delete_provider_model(model_id: int, db: Session = Depends(get_db)):
    """删除模型"""
    model = db.get(ProviderModel, model_id)
    if not model:
        raise HTTPException(status_code=404, detail="Model not found")

    db.delete(model)
    db.commit()
    return None

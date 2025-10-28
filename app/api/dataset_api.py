import os
import mimetypes
import asyncio
from io import BytesIO
from pathlib import Path

import pandas as pd
from fastapi import (
    APIRouter, Depends, HTTPException, Request, status, Query
)
from starlette.datastructures import UploadFile as StarletteUploadFile
from starlette.responses import StreamingResponse
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError
from loguru import logger

from app.core.database import SessionLocal
from app.models.dataset import Dataset
from app.schemas.dataset_schema import DatasetRead, DatasetListResponse, DatasetCreate
from app.crud.dataset_crud import DatasetCRUD
from app.utils.pressure_test_util import compute_md5_bytes, upload_to_tos, dify_api_url_2_account_profile_url, \
    dify_get_account_id
from app.core.config import settings

router = APIRouter(prefix="/datasets", tags=["Datasets"])


# ✅ 获取数据库会话
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


# ✅ 上传数据集
@router.post("/upload", response_model=DatasetRead, status_code=status.HTTP_201_CREATED)
async def upload_dataset(request: Request, db: Session = Depends(get_db)):
    """
    上传数据集文件到火山 TOS，计算 MD5 并写入数据库。
    若 (uploaded_by, file_md5, agent_id) 已存在，则直接复用。
    """
    form = await request.form()
    upload = form.get("file")
    uploaded_by = form.get("uploaded_by", "anonymous")
    agent_id = form.get("agent_id")

    account_profile_url = dify_api_url_2_account_profile_url(form.get("dify_api_url"))
    dify_account_id = dify_get_account_id(account_profile_url, uploaded_by)

    if not agent_id:
        raise HTTPException(status_code=400, detail="必须提供 agent_id")
    if not isinstance(upload, StarletteUploadFile):
        raise HTTPException(status_code=400, detail="必须包含 file 字段")

    # 读取文件
    file_bytes = await upload.read()
    suffix = Path(upload.filename).suffix.lower()
    file_md5 = compute_md5_bytes(file_bytes)
    logger.info(f"📦 上传文件 {upload.filename} 的 MD5: {file_md5}")

    # ✅ 去重逻辑：检查是否已有相同文件
    # existing = DatasetCRUD.get_by_md5(db, file_md5, agent_id=agent_id, uploaded_by=dify_account_id)
    # logger.debug(f"✅ 检查数据集: {existing}")
    # if existing:
    #     DatasetCRUD.update_by_uuid(db, existing.uuid, **{"is_deleted":False})
    #     logger.info(f"✅ 文件已存在，复用数据集: {existing.tos_url}")
    #     return existing
    # elif not existing:
    #     pass

    # ✅ 写入临时文件
    upload_dir = Path(settings.FILE_UPLOAD_DIR)
    upload_dir.mkdir(parents=True, exist_ok=True)
    tmp_path = upload_dir / f"{file_md5}{suffix}"
    tmp_path.write_bytes(file_bytes)

    # ✅ 上传到 TOS
    tos_object_key = f"datasets/{agent_id}/{file_md5}{suffix}"
    try:
        tos_url = await asyncio.to_thread(upload_to_tos, tmp_path, tos_object_key)
    except Exception as e:
        logger.exception("❌ 上传至 TOS 失败")
        raise HTTPException(status_code=500, detail=f"TOS 上传失败: {e}")
    finally:
        # 删除临时文件
        if tmp_path.exists():
            tmp_path.unlink(missing_ok=True)

    # ✅ 预览前3行
    preview_rows = []
    try:
        df = None
        if suffix == ".csv":
            df = pd.read_csv(BytesIO(file_bytes))
        elif suffix in [".xls", ".xlsx"]:
            df = pd.read_excel(BytesIO(file_bytes))
        preview_rows = df.head(3).to_dict(orient="records")
    except Exception as e:
        logger.warning(f"⚠️ 预览文件失败: {e}")

    # ✅ 写入数据库
    dataset_data = DatasetCreate(
        filename=upload.filename,
        file_md5=file_md5,
        file_suffix=suffix,
        tos_key=tos_object_key,
        tos_url=tos_url,
        preview_rows=preview_rows,
        uploaded_by=dify_account_id,
        agent_id=agent_id,
    )

    try:
        created = DatasetCRUD.create(db, dataset_data)
        logger.success(f"✅ 已保存数据集记录: {created.uuid}")
        return created
    except SQLAlchemyError as e:
        logger.exception("❌ 数据库写入失败")
        raise HTTPException(status_code=500, detail="数据库写入失败")


# ✅ 分页查询数据集
@router.get("/list", response_model=DatasetListResponse)
def list_datasets(
    uploaded_by: str = Query(None, description="按上传者过滤"),
    agent_id: str = Query(None, description="按 agent_id 过滤"),
    dify_api_url: str = Query(..., description="Dify API URL"),
    page: int = Query(1, ge=1, description="页码，从1开始"),
    limit: int = Query(50, ge=1, le=500, description="每页数量"),
    db: Session = Depends(get_db),
):
    logger.debug(f"分页查询数据集: uploaded_by={uploaded_by}, agent_id={agent_id}, dify_api_url={dify_api_url}")
    account_profile_url = dify_api_url_2_account_profile_url(dify_api_url)
    dify_account_id = dify_get_account_id(account_profile_url,uploaded_by)
    """分页列出上传的数据集"""
    offset = (page - 1) * limit
    total = DatasetCRUD.count(db, uploaded_by=dify_account_id, agent_id=agent_id)
    datasets = DatasetCRUD.list_all(db, uploaded_by=dify_account_id, agent_id=agent_id, limit=limit, offset=offset)

    return {"total": total, "page": page, "limit": limit, "items": [d.to_dict(exclude_none=True) for d in datasets]}


# ✅ 获取单个数据集详情
@router.get("/{uuid}", response_model=DatasetRead)
def get_dataset(uuid: str, db: Session = Depends(get_db)):
    dataset = DatasetCRUD.get_by_uuid(db, uuid)
    if not dataset:
        raise HTTPException(status_code=404, detail="数据集不存在")
    return dataset


# ✅ 删除数据集（逻辑删除）
@router.delete("/{uuid}", response_model=dict)
def delete_dataset(uuid: str, db: Session = Depends(get_db)):
    success = DatasetCRUD.soft_delete(db, uuid)
    if not success:
        raise HTTPException(status_code=500, detail="删除失败")
    return {"message": f"✅ 数据集 {uuid} 已标记为删除"}


# ✅ 下载数据集文件
@router.get("/download/{dataset_uuid}")
def download_dataset(dataset_uuid: str, db: Session = Depends(get_db)):
    dataset = db.query(Dataset).filter(Dataset.uuid == dataset_uuid, Dataset.is_deleted.is_(False)).first()
    if not dataset:
        raise HTTPException(status_code=404, detail="未找到对应的数据集")

    try:
        import tos
        ak, sk = os.getenv("TOS_ACCESS_KEY"), os.getenv("TOS_SECRET_KEY")
        endpoint, region, bucket = os.getenv("TOS_ENDPOINT"), os.getenv("TOS_REGION"), os.getenv("TOS_BUCKET")
        client = tos.TosClientV2(ak, sk, endpoint, region)
        obj_stream = client.get_object(bucket, dataset.tos_key)
        buffer = BytesIO()
        for chunk in obj_stream:
            buffer.write(chunk)
        buffer.seek(0)
    except Exception as e:
        logger.exception("❌ 从 TOS 下载失败")
        raise HTTPException(status_code=500, detail=f"TOS 下载失败: {e}")

    filename = dataset.filename or f"{dataset.file_md5}{dataset.file_suffix or '.csv'}"
    media_type, _ = mimetypes.guess_type(filename)
    headers = {"Content-Disposition": f'attachment; filename="{filename}"'}
    return StreamingResponse(buffer, headers=headers, media_type=media_type or "application/octet-stream")

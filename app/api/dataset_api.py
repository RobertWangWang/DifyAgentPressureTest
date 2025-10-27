import os
import mimetypes
import asyncio
from io import BytesIO

import pandas as pd
from pathlib import Path
from fastapi import APIRouter, Depends, HTTPException, UploadFile, Request, status, Query
from starlette.datastructures import UploadFile as StarletteUploadFile
from sqlalchemy.orm import Session
from sqlalchemy.exc import SQLAlchemyError
from starlette.responses import StreamingResponse

from app.core.database import SessionLocal
from app.models.dataset import Dataset
from app.schemas.dataset_schema import DatasetRead, DatasetListResponse
from app.crud.dataset_crud import DatasetCRUD
from app.utils.pressure_test_util import compute_md5_bytes, upload_to_tos
from app.core.config import settings
from loguru import logger

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
    若相同 MD5 文件已存在，则直接复用。
    """
    form = await request.form()
    upload = form.get("file")
    uploaded_by = form.get("uploaded_by", "anonymous")

    if not isinstance(upload, StarletteUploadFile):
        raise HTTPException(status_code=400, detail="必须包含 file 字段")

    # 读取文件内容
    file_bytes = await upload.read()
    suffix = Path(upload.filename).suffix.lower()
    file_md5 = compute_md5_bytes(file_bytes)
    logger.info(f"📦 上传文件 {upload.filename} 的 MD5: {file_md5}")

    # ✅ 检查数据库中是否已有相同数据集
    existing = DatasetCRUD.get_by_md5(db, file_md5)
    if existing:
        logger.info(f"✅ 文件已存在，复用数据集: {existing.tos_url}")
        return existing

    # ✅ 写入临时文件
    upload_dir = Path(settings.FILE_UPLOAD_DIR)
    upload_dir.mkdir(parents=True, exist_ok=True)
    tmp_path = upload_dir / f"{file_md5}{suffix}"
    tmp_path.write_bytes(file_bytes)
    logger.info(f"📄 已写入临时文件: {tmp_path}")

    # ✅ 上传到 TOS
    tos_object_key = f"datasets/{file_md5}{suffix}"
    tos_url = None
    try:
        tos_url = await asyncio.to_thread(upload_to_tos, tmp_path, tos_object_key)
        logger.info(f"✅ 上传至火山 TOS 成功: {tos_url}")
    except Exception as e:
        logger.error(f"❌ 上传至火山 TOS 失败: {e}")
        raise HTTPException(status_code=500, detail=f"TOS 上传失败: {e}")

    # ✅ 预览文件前 3 行（在删除文件前执行）
    preview_rows = []
    try:
        if suffix == ".csv":
            df = pd.read_csv(tmp_path)
        elif suffix in [".xls", ".xlsx"]:
            df = pd.read_excel(tmp_path)
        preview_rows = df.head(3).to_dict(orient="records")
        logger.info(f"✅ 文件 {upload.filename} 前 3 行: {preview_rows}")
    except Exception as e:
        logger.warning(f"⚠️ 预览文件内容失败（非关键步骤）{e}")

    # ✅ 上传成功后再删除本地临时文件
    try:
        if tmp_path.exists():
            tmp_path.unlink()
            logger.info(f"🧹 已删除本地临时文件: {tmp_path}")
    except Exception as e:
        logger.warning(f"⚠️ 删除本地临时文件失败: {e}")

    # ✅ 写入数据库
    try:
        from app.schemas.dataset_schema import DatasetCreate

        dataset_data = DatasetCreate(
            filename=upload.filename,
            file_md5=file_md5,
            file_suffix=suffix,
            tos_key=tos_object_key,
            tos_url=tos_url,
            preview_rows=preview_rows,
            uploaded_by=uploaded_by,
        )
        created = DatasetCRUD.create(db, dataset_data)
        logger.info(f"✅ 已保存数据集记录: {created.uuid}")
    except SQLAlchemyError as e:
        logger.error(f"❌ 数据库写入失败: {e}")
        raise HTTPException(status_code=500, detail="数据库写入失败")

    return created

# ✅ 获取数据集列表
@router.get("/list", response_model=DatasetListResponse)
def list_datasets(
    uploaded_by: str = Query(None, description="按上传者过滤"),
    page: int = Query(1, ge=1, description="页码，从1开始"),
    limit: int = Query(50, ge=1, le=500, description="每页数量"),
    db: Session = Depends(get_db),
):
    """分页列出上传的数据集"""
    # 计算偏移量
    offset = (page - 1) * limit

    # 获取总数
    total = DatasetCRUD.count(db, uploaded_by=uploaded_by)

    # 获取分页数据
    datasets = DatasetCRUD.list_all(db, uploaded_by=uploaded_by, limit=limit, offset=offset)

    return {
        "total": total,
        "page": page,
        "limit": limit,
        "items": [d.to_dict(exclude_none=True) for d in datasets],
    }


# ✅ 获取单个数据集详情
@router.get("/{uuid}", response_model=DatasetRead)
def get_dataset(uuid: str, db: Session = Depends(get_db)):
    """根据 UUID 获取单个数据集详情"""
    dataset = DatasetCRUD.get_by_uuid(db, uuid)
    if not dataset:
        raise HTTPException(status_code=404, detail="数据集不存在")
    return dataset


# ✅ 删除数据集（逻辑删除）
@router.delete("/{uuid}", response_model=dict)
def delete_dataset(uuid: str, db: Session = Depends(get_db)):
    """逻辑删除数据集"""
    success = DatasetCRUD.soft_delete(db, uuid)
    if not success:
        raise HTTPException(status_code=500, detail="删除失败")
    return {"message": f"✅ 数据集 {uuid} 已标记为删除"}

@router.get("/download/{dataset_uuid}")
def download_dataset(dataset_uuid: str, db: Session = Depends(get_db)):
    """
    ✅ 根据数据集 UUID 下载文件
    1. 查询数据库 → 获取 file_md5, tos_key
    2. 从 TOS 下载文件到内存
    3. FastAPI 以流形式返回下载
    """
    # 1️⃣ 查找数据集
    dataset = db.query(Dataset).filter(
        Dataset.uuid == dataset_uuid,
        Dataset.is_deleted.is_(False)
    ).first()

    if not dataset:
        raise HTTPException(status_code=404, detail="未找到对应的数据集")

    file_md5 = dataset.file_md5
    object_key = dataset.tos_key
    file_suffix = dataset.file_suffix or ".csv"
    filename = dataset.filename or f"{file_md5}{file_suffix}"

    logger.info(f"📦 正在下载数据集: uuid={dataset_uuid}, md5={file_md5}, key={object_key}")

    # 2️⃣ 下载文件到内存
    try:
        import tos
        import os

        ak = os.getenv("TOS_ACCESS_KEY")
        sk = os.getenv("TOS_SECRET_KEY")
        endpoint = os.getenv("TOS_ENDPOINT")
        region = os.getenv("TOS_REGION")
        bucket_name = os.getenv("TOS_BUCKET")

        client = tos.TosClientV2(ak, sk, endpoint, region)
        obj_stream = client.get_object(bucket_name, object_key.__str__())
        buffer = BytesIO()

        for chunk in obj_stream:
            buffer.write(chunk)
        buffer.seek(0)

        logger.success(f"✅ 成功下载至内存: {filename}")

    except Exception as e:
        logger.error(f"❌ 从 TOS 下载失败: {e}")
        raise HTTPException(status_code=500, detail=f"TOS 下载失败: {e}")

    # 3️⃣ 返回 StreamingResponse，让浏览器下载
    media_type, _ = mimetypes.guess_type(filename)
    media_type = media_type or "application/octet-stream"

    headers = {
        "Content-Disposition": f'attachment; filename="{filename}"',
        "Content-Type": media_type,
    }

    return StreamingResponse(buffer, headers=headers, media_type=media_type)

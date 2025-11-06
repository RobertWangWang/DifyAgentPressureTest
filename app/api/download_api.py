from pathlib import Path
from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import FileResponse, HTMLResponse
from sqlalchemy.orm import Session

from app.core.database import SessionLocal
from app.crud.test_record_crud import TestRecordCRUD
from app.utils.logger import logger

router = APIRouter(prefix="/download", tags=["Download"])

# 上传文件目录（根据你的结构）
BASE_DIR = Path(__file__).resolve().parent.parent.parent
UPLOAD_DIR = BASE_DIR / "uploads"

# 永久 token 映射表（服务重启后会丢失）
DOWNLOAD_TOKENS = {}  # {token: file_path}


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


@router.get("/{uuid}")
def download_file(uuid: str, db: Session = Depends(get_db)):
    """
    根据 TestRecord.uuid 从数据库获取 dataset_absolute_path，
    返回文件下载响应。
    """
    record = TestRecordCRUD.get_by_uuid(db, uuid)
    if not record:
        raise HTTPException(status_code=404, detail=f"Record with uuid {uuid} not found")

    # 优先从 dataset_absolute_path 获取路径
    file_path = record.dataset_absolute_path
    if not file_path:
        raise HTTPException(status_code=404, detail="dataset_absolute_path is not set for this record")

    file_path = Path(file_path)
    if not file_path.exists():
        raise HTTPException(status_code=404, detail=f"File not found on server: {file_path}")

    logger.info(f"📦 用户请求下载文件: {file_path}")

    return FileResponse(
        path=str(file_path),
        filename=file_path.name,
        media_type="application/octet-stream"
    )
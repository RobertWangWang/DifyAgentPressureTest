import uuid as uuidlib
import os
from pathlib import Path
from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import FileResponse, HTMLResponse
from sqlalchemy.orm import Session
from loguru import logger

from app.core.database import SessionLocal
from app.models.test_record import TestRecord  # 根据你的路径调整

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


@router.get("/generate/{uuid}")
def generate_download_link(uuid: str, db: Session = Depends(get_db)):
    """
    根据数据库记录生成永久下载链接
    """
    record = db.query(TestRecord).filter(TestRecord.uuid == uuid).first()
    if not record:
        raise HTTPException(status_code=404, detail="Record not found")

    file_path = UPLOAD_DIR / record.filename
    if not file_path.exists():
        raise HTTPException(status_code=404, detail=f"File not found on server: {file_path}")

    # 如果该文件已有 token，复用
    for tk, path in DOWNLOAD_TOKENS.items():
        if path == str(file_path):
            logger.info(f"[Download] 已存在token: {tk} -> {file_path}")
            return {"download_url": f"/download/file/{tk}"}

    # 否则生成新 token
    token = str(uuidlib.uuid4())
    DOWNLOAD_TOKENS[token] = str(file_path)

    logger.info(f"[Download] 新token: {token} -> {file_path}")
    return {"download_url": f"/download/file/{token}"}


@router.get("/file/{token}")
def download_file(token: str):
    """
    用户点击链接后下载文件（永久有效）
    """
    file_path = DOWNLOAD_TOKENS.get(token)
    if not file_path:
        raise HTTPException(status_code=404, detail="Invalid token")

    if not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail="File missing on server")

    filename = os.path.basename(file_path)

    logger.info(f"[Download] 用户下载: {filename}")
    return FileResponse(
        path=file_path,
        filename=filename,
        media_type="application/octet-stream"
    )


@router.get("/auto/{token}")
def auto_download(token: str):
    """
    自动跳转下载页面
    """
    if token not in DOWNLOAD_TOKENS:
        raise HTTPException(status_code=404, detail="Invalid token")

    html = f"""
    <html>
        <head>
            <meta http-equiv="refresh" content="0; url=/download/file/{token}">
        </head>
        <body>
            <p>正在自动下载... <a href="/download/file/{token}">如果没有自动下载，请点击这里</a></p>
        </body>
    </html>
    """
    return HTMLResponse(content=html)

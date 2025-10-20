from pathlib import Path
from typing import Any, Dict, List, Optional

import asyncio
import json
from time import perf_counter

from starlette.datastructures import UploadFile as StarletteUploadFile
from fastapi import APIRouter, Depends, HTTPException, Request, UploadFile, status
from fastapi.concurrency import run_in_threadpool
from pydantic import ValidationError
from sqlalchemy.orm import Session

from app.crud.test_chatflow_record_crud import TestRecordCRUD
from app.schemas.test_record_schema import (
    TestRecordCreate,
    TestRecordRead,
    TestRecordUpdate,
)
from app.core.config import settings
from app.core.database import SessionLocal
from app.services.test_record_services import test_chatflow_non_stream_pressure_wrapper
from app.models.test_chatflow_record import TestStatus

# ✅ 导入 util 工具函数
from app.utils.pressure_test import (
    dify_api_url_2_agent_apikey_url,
    create_dify_agent_api_key,
)

from loguru import logger

router = APIRouter(prefix="/test_chatflow_records", tags=["TestChatflowRecords"])

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


@router.post("/", response_model=TestRecordRead, status_code=status.HTTP_201_CREATED)
async def create_record(request: Request, db: Session = Depends(get_db)):
    content_type = request.headers.get("content-type", "").lower()

    if not content_type.startswith("multipart/form-data"):
        raise HTTPException(
            status_code=415,
            detail="Request content type must be multipart/form-data with a file upload.",
        )

    async def _persist_upload(upload: UploadFile) -> str:
        upload_dir = Path(settings.FILE_UPLOAD_DIR)
        upload_dir.mkdir(parents=True, exist_ok=True)

        original_filename = Path(upload.filename or "").name
        if not original_filename:
            raise HTTPException(status_code=400, detail="Uploaded file must have a name.")

        stem = Path(original_filename).stem
        suffix = Path(original_filename).suffix
        candidate_name = original_filename
        candidate_path = upload_dir / candidate_name
        counter = 1
        while candidate_path.exists():
            candidate_name = f"{stem}_{counter}{suffix}"
            candidate_path = upload_dir / candidate_name
            counter += 1

        file_bytes = await upload.read()
        candidate_path.write_bytes(file_bytes)
        await upload.close()
        return candidate_name

    # 1️⃣ 解析 form-data 表单
    form = await request.form()
    upload = form.get("file")

    if not isinstance(upload, StarletteUploadFile):
        raise HTTPException(
            status_code=400,
            detail="A 'file' upload is required and must be provided as a file.",
        )

    payload_data: dict[str, str] = {}
    for key, value in form.multi_items():
        if key == "file":
            continue
        payload_data[key] = value

    # 2️⃣ 解析 payload 为 Pydantic 模型
    try:
        record_payload = TestRecordCreate(**payload_data)
    except ValidationError as exc:
        raise HTTPException(status_code=422, detail=exc.errors()) from exc

    # 3️⃣ 生成 API Key（调用 util 函数）
    try:
        # 拼接 agent 的 API key URL
        api_key_url = dify_api_url_2_agent_apikey_url(
            record_payload.dify_api_url,
            record_payload.dify_test_agent_id,
        )

        # 创建 API Key
        created_key = create_dify_agent_api_key(
            api_key_url,
            record_payload.dify_bearer_token,
        )

        # 写入 record payload
        token_value = created_key.get("token")
        if not token_value:
            raise ValueError(f"Dify 返回无效 API Key：{created_key}")

        record_payload.dify_api_key = token_value
        logger.success(f"✅ Dify API Key created for agent: {record_payload.dify_test_agent_id}")

    except Exception as e:
        logger.error(f"❌ 生成 Dify API Key 失败: {e}")
        raise HTTPException(status_code=500, detail=f"创建 Dify API Key 失败: {e}")

    # 4️⃣ 保存文件到本地
    filename = await _persist_upload(upload)

    # 5️⃣ 写入数据库
    created = TestRecordCRUD.create(
        db,
        filename=filename,
        **record_payload.dict()
    )

    return created

@router.get("/{uuid_str}", response_model=TestRecordRead)
def get_record(uuid_str: str, db: Session = Depends(get_db)):
    rec = TestRecordCRUD.get_by_uuid(db, uuid_str)
    if rec is None:
        raise HTTPException(status_code=404, detail="Record not found")
    return rec


@router.get("/", response_model=List[TestRecordRead])
def list_records(limit: int = 100, db: Session = Depends(get_db)):
    return TestRecordCRUD.list_all(db, limit=limit)


@router.patch("/{uuid_str}", response_model=TestRecordRead)
def update_record(uuid_str: str, payload: TestRecordUpdate, db: Session = Depends(get_db)):
    existing = TestRecordCRUD.get_by_uuid(db, uuid_str)
    if existing is None:
        raise HTTPException(status_code=404, detail="Record not found")
    updated = TestRecordCRUD.update_by_uuid(db, uuid_str, **payload.dict(exclude_unset=True))
    return updated


@router.delete("/{uuid_str}", status_code=status.HTTP_204_NO_CONTENT)
def delete_record(uuid_str: str, db: Session = Depends(get_db)):
    existing = TestRecordCRUD.get_by_uuid(db, uuid_str)
    if existing is None:
        raise HTTPException(status_code=404, detail="Record not found")
    success = TestRecordCRUD.delete_by_uuid(db, uuid_str)
    if not success:
        raise HTTPException(status_code=500, detail="Delete failed")
    return None


@router.post("/run_test/{uuid_str}", status_code=status.HTTP_200_OK)
async def run_record(
    request: Request, uuid_str: str, db: Session = Depends(get_db)
):
    existing = await run_in_threadpool(TestRecordCRUD.get_by_uuid, db, uuid_str)
    if existing is None:
        raise HTTPException(status_code=404, detail="Record not found")

    await run_in_threadpool(
        TestRecordCRUD.update_by_uuid, db, uuid_str, status=TestStatus.RUNNING
    )

    llm = request.session.get("llm")
    started_at = perf_counter()

    async def _progress_callback(payload: Dict[str, Any]) -> None:
        logger.info(f"Record {uuid_str} progress: {payload}")

    def _finalize_record(
        status: TestStatus,
        *,
        result_payload: Optional[Dict[str, Any]] = None,
        duration: Optional[int] = None,
        error_message: Optional[str] = None,
    ) -> None:
        session = SessionLocal()
        try:
            update_kwargs: Dict[str, Any] = {"status": status}
            if duration is not None:
                update_kwargs["duration"] = duration

            if result_payload is not None:
                update_kwargs["result"] = json.dumps(
                    result_payload, ensure_ascii=False
                )
            elif error_message is not None:
                update_kwargs["result"] = error_message

            TestRecordCRUD.update_by_uuid(session, uuid_str, **update_kwargs)
        finally:
            session.close()

    async def _background_run() -> None:
        try:
            result = await test_chatflow_non_stream_pressure_wrapper(
                existing,
                llm=llm,
                progress_callback=_progress_callback,
            )
            logger.info(f"Record {uuid_str} completed: {result}")
            duration = int(perf_counter() - started_at)
            await asyncio.to_thread(
                _finalize_record,
                TestStatus.INIT,
                result_payload=result,
                duration=duration,
            )
        except Exception as exc:  # pragma: no cover - background logging
            logger.exception(f"Record {uuid_str} failed during execution: {exc}")
            await asyncio.to_thread(
                _finalize_record,
                TestStatus.FAILED,
                error_message=str(exc),
            )

    asyncio.create_task(_background_run())

    return {"status": "accepted", "uuid": uuid_str}

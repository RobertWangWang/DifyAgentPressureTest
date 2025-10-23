import pandas as pd
from pathlib import Path
from typing import List

from starlette.datastructures import UploadFile as StarletteUploadFile
from fastapi import APIRouter, Depends, HTTPException, Request, UploadFile, status, Query
from fastapi.responses import FileResponse
from starlette.responses import JSONResponse
from fastapi import BackgroundTasks
from fastapi.concurrency import run_in_threadpool
from pydantic import ValidationError
from sqlalchemy.orm import Session

from app.crud.test_record_crud import TestRecordCRUD
from app.models.provider_model import ProviderModel
from app.schemas.test_record_schema import (
    TestRecordCreate,
    TestRecordRead,
    TestRecordUpdate,
    PaginatedTestRecordResponse,
    TestRecordsByUUIDAndBearerToken,
)
from app.core.config import settings
from app.core.database import SessionLocal
from app.services.test_record_services import (
    test_chatflow_non_stream_pressure_wrapper,
    test_workflow_non_stream_pressure_wrapper,
)
from app.services.provider_model_services import llm_connection_test

# ✅ 导入 util 工具函数
from app.utils.pressure_test_util import (
    dify_api_url_2_agent_apikey_url,
    dify_api_url_2_agent_api_app_url,
    dify_get_agent_type_and_agent_name,
    create_dify_agent_api_key,
    get_dify_agent_api_key,
    dify_get_account_id,
    dify_api_url_2_account_profile_url
)

from loguru import logger

router = APIRouter(prefix="/test_records", tags=["TestChatflowRecords"])

def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


@router.post("/", response_model=dict, status_code=status.HTTP_201_CREATED)
async def create_record(request: Request, db: Session = Depends(get_db)):
    """接收文件 + 表单参数，返回文件前三行内容并继续写入数据库"""
    content_type = request.headers.get("content-type", "").lower()

    if not content_type.startswith("multipart/form-data"):
        raise HTTPException(
            status_code=415,
            detail="Request content type must be multipart/form-data with a file upload.",
        )

    async def _persist_upload(upload: UploadFile) -> Path:
        """保存上传文件到本地并返回路径"""
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
        return candidate_path

    # 1️⃣ 解析 form-data
    form = await request.form()
    upload = form.get("file")

    if not isinstance(upload, StarletteUploadFile):
        raise HTTPException(status_code=400, detail="A 'file' upload is required and must be provided as a file.")

    payload_data: dict[str, str] = {}
    for key, value in form.multi_items():
        if key != "file":
            payload_data[key] = value

    # 2️⃣ 保存文件
    file_path = await _persist_upload(upload)

    # 3️⃣ 读取文件前 3 条内容
    preview_rows = []
    try:
        suffix = file_path.suffix.lower()
        if suffix == ".csv":
            df = pd.read_csv(file_path)
        elif suffix in [".xls", ".xlsx"]:
            df = pd.read_excel(file_path)
        else:
            raise HTTPException(status_code=400, detail=f"Unsupported file type: {suffix}")

        preview_rows = df.head(3).to_dict(orient="records")
        logger.info(f"✅ 文件 {file_path.name} 前 3 行内容: {preview_rows}")
    except Exception as e:
        logger.error(f"❌ 文件解析失败: {e}")
        raise HTTPException(status_code=400, detail=f"无法读取文件内容: {e}")

    # 4️⃣ 解析 payload
    try:
        record_payload = TestRecordCreate(**payload_data)
    except ValidationError as exc:
        raise HTTPException(status_code=422, detail=exc.errors()) from exc

    # 5️⃣ 生成 Dify API Key
    try:
        api_key_url = dify_api_url_2_agent_apikey_url(
            record_payload.dify_api_url, record_payload.dify_test_agent_id
        )
        existing_keys = get_dify_agent_api_key(api_key_url, record_payload.dify_bearer_token)
        if existing_keys:
            token_value = existing_keys[0].get("token")
            logger.debug("✅ 使用已有 API Key")
        else:
            created_key = create_dify_agent_api_key(api_key_url, record_payload.dify_bearer_token)
            token_value = created_key.get("token")
        if not token_value:
            raise ValueError("Dify 返回无效 API Key")
        record_payload.dify_api_key = token_value
    except Exception as e:
        logger.error(f"❌ 生成 Dify API Key 失败: {e}")
        raise HTTPException(status_code=500, detail=f"创建 Dify API Key 失败: {e}")

    # 6️⃣ 获取 Agent 信息
    try:
        agent_api_app_url = dify_api_url_2_agent_api_app_url(
            record_payload.dify_api_url, record_payload.dify_test_agent_id
        )
        data_dict = dify_get_agent_type_and_agent_name(agent_api_app_url, record_payload.dify_bearer_token)
        record_payload.agent_type = data_dict["agent_type"]
        record_payload.agent_name = data_dict["agent_name"]
    except Exception as e:
        logger.error(f"❌ 获取 Agent 类型失败: {e}")
        raise HTTPException(status_code=500, detail=f"获取 Dify Agent 类型失败: {e}")

    # 7️⃣ 获取 Account ID
    try:
        dify_account_profile_url = dify_api_url_2_account_profile_url(record_payload.dify_api_url)
        record_payload.dify_account_id = dify_get_account_id(
            dify_account_profile_url, record_payload.dify_bearer_token
        )
    except Exception as e:
        logger.error(f"❌ 获取 Dify Account ID 失败: {e}")
        raise HTTPException(status_code=500, detail=f"获取 Dify Account ID 失败: {e}")

    ### 获取llm到seession
    judge_model = form.get('judge_model')
    judge_model_provider_name = form.get('judge_model_provider_name')
    llm_models = (
        db.query(ProviderModel)
        .filter(
            ProviderModel.provider_name == judge_model_provider_name,
            ProviderModel.model_name == judge_model,
        )
        .all()
    )
    llm = llm_connection_test(candidate_models= llm_models)
    request.session['llm'] = llm
    ###

    # 8️⃣ 写入数据库
    created = TestRecordCRUD.create(db, filename=file_path.name, **record_payload.model_dump())

    request.session['dify_agent_pressure_task_uuid'] = created.uuid



    # ✅ 返回结果（包含文件前三行）
    return JSONResponse(
        content={
            "message": "✅ 文件上传并创建记录成功",
            "uploaded_filename": file_path.name,
            "file_preview": preview_rows,
            "created_record": created.to_dict(exclude_none=True),
        }
    )

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


@router.get(
    "/get_records_by_agent_id/{agent_id}",
    response_model=PaginatedTestRecordResponse,
    status_code=status.HTTP_200_OK
)
def get_records_by_agent_id(
    agent_id: str,
    page: int = Query(1, ge=1, description="页码，从1开始"),
    page_size: int = Query(10, ge=1, le=100, description="每页返回的条目数，默认10，最大100"),
):
    return TestRecordCRUD.get_all_records_by_agent_id(agent_id, page, page_size)


@router.get("/get_record_by_task_name/{input_task_name}",
            response_model=PaginatedTestRecordResponse,
            status_code=status.HTTP_200_OK
            )
def get_record_by_task_name(input_task_name: str, page: int = Query(1, ge=1, description="页码，从1开始"),
                            page_size: int = Query(10, ge=1, le=100, description="每页返回的条目数，默认10，最大100")):
    return TestRecordCRUD.get_all_records_by_task_name(input_task_name, page, page_size)

@router.post("/run_test/{uuid_str}", status_code=status.HTTP_200_OK)
async def run_record(
        request: Request,
        uuid_str: str,
        background_tasks: BackgroundTasks,db: Session = Depends(get_db),
        mode: str = Query(default="all", description="experiment or full mode+"),
):
    existing = await run_in_threadpool(TestRecordCRUD.get_by_uuid, db, uuid_str)
    if existing is None:
        raise HTTPException(status_code=404, detail="Record not found")

    if existing.status == "running":
        return {"error": "测试任务正在运行中"}
    elif existing.status == "success":
        return existing.result

    if existing.agent_type == "chatflow":
        background_tasks.add_task(test_chatflow_non_stream_pressure_wrapper, existing, request, db, mode)
    elif  existing.agent_type == "workflow":
        background_tasks.add_task(test_workflow_non_stream_pressure_wrapper, existing, request, db, mode)

    return JSONResponse(content={
        "status": "running",
        "message": "测试已在后台启动 ✅"
    })

@router.get("/get_dataset_first_three_lines/{uuid_str}", status_code=status.HTTP_200_OK)
def get_dataset_first_three_lines(uuid_str: str):

    return TestRecordCRUD.get_dataset_first_three_lines(uuid_str)

@router.post("/get_records_by_uuid_and_bearer_token",status_code=status.HTTP_200_OK,response_model=List[TestRecordRead])
def get_records_by_uuid_and_bearer_token(request: TestRecordsByUUIDAndBearerToken):

    return TestRecordCRUD.get_records_by_uuid_and_bearer_token(request.agent_id,request.bearer_token)
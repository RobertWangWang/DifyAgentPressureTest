import pandas as pd
import io
from pathlib import Path
from typing import List
from starlette.datastructures import UploadFile as StarletteUploadFile
from fastapi import APIRouter, Depends, HTTPException, Request, UploadFile, status, Query, Body, BackgroundTasks
from starlette.responses import JSONResponse, StreamingResponse
from fastapi.concurrency import run_in_threadpool
from pydantic import ValidationError
from sqlalchemy.orm import Session
import asyncio
from concurrent.futures import ThreadPoolExecutor
executor = ThreadPoolExecutor(max_workers=128)
asyncio.get_event_loop().set_default_executor(executor)
import hashlib
import os

from app.crud.test_record_crud import TestRecordCRUD
from app.crud.dataset_crud import DatasetCRUD
from app.models.provider_model import ProviderModel
from app.models.test_record import TestRecord, TestStatus, AgentType
from app.schemas.test_record_schema import (
    TestRecordCreate,
    TestRecordRead,
    TestRecordUpdate,
    PaginatedTestRecordResponse,
    TestRecordsByUUIDAndBearerToken,
    TestRecordStatus,
    AgentParameterRequest,
)
from app.schemas.dataset_schema import DatasetCreate, DatasetRead
from app.core.config import settings
from app.core.database import SessionLocal,AsyncSessionLocal
from app.services.test_record_services import (
    test_chatflow_non_stream_pressure_wrapper,
    test_chatflow_stream_pressure_wrapper,
    test_workflow_non_stream_pressure_wrapper,

)

from app.core.celery_app import celery_app
from app.services.provider_model_services import llm_connection_test
from app.utils.pressure_test_util import (
    dify_api_url_2_agent_apikey_url,
    dify_api_url_2_agent_api_app_url,
    dify_get_agent_type_and_agent_name,
    create_dify_agent_api_key,
    get_dify_agent_api_key,
    dify_get_account_id,
    dify_api_url_2_account_profile_url,
    get_workflow_parameter_template,
    get_chatflow_parameter_template,
    get_chat_parameter_template,
    get_agent_chat_parameter_template,
    get_completion_parameter_template,
    upload_to_tos,
)
from app.utils.logger import logger

DIFY_API_URL = os.environ.get("TARGET_DIFY_API_URL")
DIFY_CONCURRENCY = int(os.environ.get("CONCURRENCY", "8"))
logger.warning(f"test_record_api.py 当前环境的dify api url为：{DIFY_API_URL}")
router = APIRouter(prefix="/test_records", tags=["TestChatflowRecords"])


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def compute_md5_bytes(data: bytes) -> str:
    """计算文件内容的 MD5"""
    md5 = hashlib.md5()
    md5.update(data)
    return md5.hexdigest()


# ==========================================================
# ✅ 上传数据集文件（Dataset 拆表后）
# ==========================================================
@router.post("/upload", response_model=DatasetRead, status_code=status.HTTP_201_CREATED)
async def upload_dataset(request: Request, db: Session = Depends(get_db)):
    """
    上传数据集文件到火山 TOS，计算 MD5 并写入 Dataset 表。
    若相同 MD5 文件已存在，则直接复用。
    """
    form = await request.form()
    upload = form.get("file")
    uploaded_by = form.get("uploaded_by", "anonymous")

    if not isinstance(upload, StarletteUploadFile):
        raise HTTPException(status_code=400, detail="必须包含 file 字段")

    # ✅ 读取文件内容并计算 MD5
    file_bytes = await upload.read()
    suffix = Path(upload.filename).suffix.lower()
    file_md5 = compute_md5_bytes(file_bytes)
    logger.info(f"📦 上传文件 {upload.filename} 的 MD5: {file_md5}")

    # ✅ 查重逻辑
    existing = DatasetCRUD.get_by_md5(db, file_md5)
    if existing:
        logger.info(f"✅ 文件已存在，复用数据集: {existing.tos_url}")
        return existing

    # ✅ 写入临时文件
    upload_dir = Path(settings.FILE_UPLOAD_DIR)
    upload_dir.mkdir(parents=True, exist_ok=True)
    tmp_path = upload_dir / f"{file_md5}{suffix}"
    tmp_path.write_bytes(file_bytes)

    # ✅ 上传至 TOS
    tos_key = f"datasets/{file_md5}{suffix}"
    try:
        tos_url = await asyncio.to_thread(upload_to_tos, tmp_path, tos_key)
        logger.info(f"✅ 上传至 TOS 成功: {tos_url}")
    except Exception as e:
        logger.error(f"❌ 上传至 TOS 失败: {e}")
        raise HTTPException(status_code=500, detail=f"TOS 上传失败: {e}")
    finally:
        if tmp_path.exists():
            tmp_path.unlink()
            logger.info(f"🧹 已删除本地临时文件: {tmp_path}")

    # ✅ 生成文件预览
    preview_rows = []
    try:
        if suffix == ".csv":
            df = pd.read_csv(io.BytesIO(file_bytes))
        elif suffix in [".xls", ".xlsx"]:
            df = pd.read_excel(io.BytesIO(file_bytes))
        preview_rows = df.head(3).to_dict(orient="records")
    except Exception as e:
        logger.warning(f"⚠️ 预览文件内容失败（非关键步骤）{e}")

    # ✅ 写入 Dataset 表
    dataset = DatasetCreate(
        filename=upload.filename,
        file_md5=file_md5,
        file_suffix=suffix,
        tos_key=tos_key,
        tos_url=tos_url,
        preview_rows=preview_rows,
        uploaded_by=uploaded_by,
    )
    created = DatasetCRUD.create(db, dataset)
    logger.info(f"✅ 数据集写入成功: uuid={created.uuid}")
    return created


# ==========================================================
# ✅ 创建评测任务（引用 dataset_uuid）
# ==========================================================
@router.post("/create_record", response_model=dict, status_code=status.HTTP_201_CREATED)
async def create_record(request: Request, db: Session = Depends(get_db)):
    """
    创建评测任务（通过 dataset_file_md5 引用 Dataset）
    - 不要求 dataset_uuid
    - agent_type 和 agent_name 由 Dify 自动补全
    - 同步创建 Dify API Key 并验证 LLM 连接
    """
    json_data = await request.json()
    try:
        record_payload = TestRecordCreate(**json_data)
    except ValidationError as e:
        raise HTTPException(status_code=422, detail=e.errors())

    file_uuid = json_data.get("dataset_file_uuid")
    if not file_uuid:
        raise HTTPException(status_code=400, detail="必须提供 dataset_file_md5")

    # ✅ 1. 查找数据集
    dataset_info = DatasetCRUD.get_by_uuid(db, file_uuid)
    logger.debug(dataset_info)
    if not dataset_info:
        raise HTTPException(status_code=404, detail="未找到对应数据集，请先上传")

    # ✅ 2. 生成 Dify API Key
    try:
        api_key_url = dify_api_url_2_agent_apikey_url(
            DIFY_API_URL, record_payload.dify_test_agent_id
        )
        existing_keys = get_dify_agent_api_key(api_key_url, record_payload.dify_bearer_token)
        token_value = (
            existing_keys[0].get("token")
            if existing_keys
            else create_dify_agent_api_key(api_key_url, record_payload.dify_bearer_token).get("token")
        )
        if not token_value:
            raise ValueError("Dify 返回无效 API Key")
        record_payload.dify_api_key = token_value
        logger.info("✅ 已生成 Dify API Key")
    except Exception as e:
        logger.error(f"❌ 创建 Dify API Key 失败: {e}")
        raise HTTPException(status_code=500, detail=f"创建 Dify API Key 失败: {e}")

    # ✅ 3. 创建 TestRecord 基础记录（agent_type/agent_name 暂为空）
    payload_dict = record_payload.model_dump(exclude={"agent_type", "agent_name"})
    payload_dict['dataset_uuid'] = dataset_info.uuid
    payload_dict['concurrency'] = DIFY_CONCURRENCY
    payload_dict['dify_api_url'] = DIFY_API_URL
    created = TestRecordCRUD.create(
        db,
        filename=dataset_info.filename,
        dataset_tos_key=dataset_info.tos_key,
        dataset_tos_url=dataset_info.tos_url,
        dify_account_id="",  # ✅ 占位
        agent_type=AgentType.CHATFLOW,  # ✅ 占位
        agent_name="",  # ✅ 占位
        **payload_dict,
    )
    logger.info(f"✅ 已创建测试任务记录 uuid={created.uuid}, dataset_uuid={dataset_info.uuid}")

    # ✅ 4. 获取 Agent 类型与名称
    try:
        agent_api_app_url = dify_api_url_2_agent_api_app_url(
            DIFY_API_URL, record_payload.dify_test_agent_id
        )
        info = dify_get_agent_type_and_agent_name(agent_api_app_url, record_payload.dify_bearer_token)
        agent_type = info.get("agent_type")
        agent_name = info.get("agent_name")

        if agent_type and agent_name:
            TestRecordCRUD.update_by_uuid(db, created.uuid, agent_type=agent_type, agent_name=agent_name)
            logger.info(f"✅ 已更新 agent_type={agent_type}, agent_name={agent_name}")
    except Exception as e:
        logger.warning(f"⚠️ 获取 Agent 信息失败（非关键步骤）: {e}")

    # ✅ 5. 获取 Dify Account ID
    try:
        account_profile_url = dify_api_url_2_account_profile_url(DIFY_API_URL)
        dify_account_id = dify_get_account_id(account_profile_url, record_payload.dify_bearer_token)
        TestRecordCRUD.update_by_uuid(db, created.uuid, dify_account_id=dify_account_id)
        logger.info(f"✅ 已更新 Dify Account ID: {dify_account_id}")
    except Exception as e:
        logger.warning(f"⚠️ 获取 Dify Account ID 失败（非关键步骤）: {e}")

    # ✅ 6. 验证 LLM 可用性
    try:
        judge_model = record_payload.judge_model
        provider = record_payload.judge_model_provider_name
        llm_models = (
            db.query(ProviderModel)
            .filter(ProviderModel.provider_name == provider, ProviderModel.model_name == judge_model)
            .all()
        )
        logger.info(f"✅ 获取 LLM 模型信息: {llm_models}")
        llm = llm_connection_test(candidate_models=llm_models)
        request.session["llm"] = llm
        logger.info(f"✅ LLM 模型连接测试通过, {llm}")
    except Exception as e:
        logger.warning(f"⚠️ LLM 模型连接失败（非关键步骤）: {e}")

    # ✅ 7. 返回结果
    return JSONResponse(
        content={
            "message": "✅ 评测任务创建成功",
            "record_uuid": created.uuid,
            "dataset_file_md5": file_uuid,
            "dataset_tos_url": dataset_info.tos_url,
            "created_at": created.created_at.isoformat(),
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



@router.delete("/{uuid_str}", response_model=TestRecordRead, status_code=status.HTTP_200_OK)
def delete_record(uuid_str: str, db: Session = Depends(get_db)):
    existing = TestRecordCRUD.get_by_uuid(db, uuid_str)
    if existing is None:
        raise HTTPException(status_code=404, detail="Record not found")

    success = TestRecordCRUD.delete_by_uuid(db, uuid_str)
    if not success:
        raise HTTPException(status_code=500, detail="Delete failed")

    # ✅ 再查询一次（即使被标记为删除）
    deleted_record = TestRecordCRUD.get_by_uuid_include_deleted(db, uuid_str)
    if not deleted_record:
        raise HTTPException(status_code=404, detail="Deleted record not found")

    return TestRecordRead.model_validate(deleted_record)


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

# ✅ 限制最大并发任务数，防止压测太多导致线程爆满
MAX_CONCURRENT_TASKS = 64
semaphore = asyncio.Semaphore(MAX_CONCURRENT_TASKS)

async def run_background_task(func, existing, request, db, mode):
    """真正的异步后台执行函数"""
    async with semaphore:
        try:
            logger.info(f"开始执行任务 {existing.uuid}, 类型={existing.agent_type}, 模式={mode}")
            await func(existing, request, db, mode)
            logger.info(f"✅ 任务 {existing.uuid} 执行完成")
        except Exception as e:
            logger.exception(f"❌ 任务 {existing.uuid} 执行失败: {e}")

@router.post("/run_test/{uuid_str}", status_code=200)
async def run_record(
    request: Request,
    uuid_str: str,
    db=Depends(get_db),
    mode: str = Query(default="all", description="experiment or full mode+"),
):
    """
    🚀 改造后：将测试任务推送到 Celery 队列执行（非阻塞）
    """
    # === 1️⃣ 检查任务记录 ===
    existing = await asyncio.to_thread(TestRecordCRUD.get_by_uuid, db, uuid_str)
    if existing is None:
        raise HTTPException(status_code=404, detail="Record not found")

    # === 2️⃣ 状态判断 ===
    if existing.status == "running":
        return {"error": "测试任务正在运行中"}
    elif existing.status in ["cancelled", "failed"]:
        return {"error": f"测试任务状态异常：{existing.status}"}
    elif existing.status in ["success", "experiment"]:
        return existing.result

    # === 3️⃣ 根据类型选择任务名称 ===
    if existing.agent_type in ["chatflow", AgentType.CHAT, AgentType.COMPLETION]:
        task_name = "tasks.run_chatflow_test"
    elif existing.agent_type == "workflow":
        task_name = "tasks.run_workflow_test"
    elif existing.agent_type == AgentType.AGENT_CHAT:
        task_name = "tasks.run_chatflow_stream_test"
    else:
        raise HTTPException(status_code=400, detail=f"未知 agent_type: {existing.agent_type}")

    # === 4️⃣ 更新状态为 pending ===
    await asyncio.to_thread(TestRecordCRUD.update_by_uuid, db, uuid_str, status="pending")

    llm_info = request.session.get("llm", {})
    # === 5️⃣ 提交任务到 Celery ===
    task = celery_app.send_task(
        task_name,
        args=[llm_info,uuid_str, mode],
        queue="default"
    )

    logger.info(f"📤 已将任务 {uuid_str} 投递至 Celery 队列 (task_id={task.id})")

    return JSONResponse(content={
        "uuid": uuid_str,
        "task_id": task.id,
        "status": "queued",
        "message": f"任务 {uuid_str} 已加入 Celery 队列等待执行 ✅"
    })

@router.get("/get_dataset_first_three_lines/{uuid_str}", status_code=status.HTTP_200_OK)
def get_dataset_first_three_lines(uuid_str: str):

    return TestRecordCRUD.get_dataset_first_three_lines(uuid_str)

@router.post("/get_records_by_uuid_and_bearer_token",status_code=status.HTTP_200_OK,response_model=List[TestRecordRead])
def get_records_by_uuid_and_bearer_token(request: TestRecordsByUUIDAndBearerToken):

    return TestRecordCRUD.get_records_by_uuid_and_bearer_token(request.agent_id,request.bearer_token)

@router.post("/get_uuid_task_status/{uuid}",status_code=status.HTTP_200_OK,response_model=TestRecordStatus)
def get_uuid_task_status(uuid: str):

    return TestRecordCRUD.get_uuid_task_status(uuid)



@router.post(
    "/search_by_keyword",
    response_model=PaginatedTestRecordResponse,
    status_code=status.HTTP_200_OK,
)
async def search_by_keyword(
    payload: dict = Body(..., example={"key_word": "", "page": 1, "page_size": 10})
):
    key_word = payload.get("key_word", "")
    page = payload.get("page", 1)
    page_size = payload.get("page_size", 10)
    console_token = payload.get("console_token", "")

    if not isinstance(page, int) or not isinstance(page_size, int):
        raise HTTPException(status_code=400, detail="page 和 page_size 必须为整数")
    if not console_token:
        raise HTTPException(status_code=400, detail="console_token 不能为空")

    async with AsyncSessionLocal() as session:
        result = await TestRecord.get_records_by_keyword_async(session,
                                                               key_word,
                                                               page,
                                                               page_size,
                                                               console_token)
        return result

@router.post("/get_parameter_template_by_agent_id",status_code=status.HTTP_200_OK)
def get_parameter_template_by_agent_id(payload: AgentParameterRequest):


    agent_id = payload.agent_id
    dify_api_url = DIFY_API_URL
    bearer_token = payload.bearer_token

    dify_app_url = dify_api_url_2_agent_api_app_url(dify_api_url, agent_id)
    agent_related_info = dify_get_agent_type_and_agent_name(input_agent_manipulate_url=dify_app_url,
                                                            input_bearer_token=bearer_token)
    agent_type = agent_related_info.get("agent_type")
    api_key = ""

    # 5️⃣ 生成 Dify API Key
    try:
        api_key_url = dify_api_url_2_agent_apikey_url(
            dify_api_url, agent_id
        )
        existing_keys = get_dify_agent_api_key(api_key_url, bearer_token)
        if existing_keys:
            api_key = existing_keys[0].get("token")
            logger.debug("✅ 使用已有 API Key")
        else:
            created_key = create_dify_agent_api_key(api_key_url, bearer_token)
            api_key = created_key.get("token")
        if not api_key:
            raise ValueError("Dify 返回无效 API Key")
    except Exception as e:
        logger.error(f"❌ 生成 Dify API Key 失败: {e}")
        raise HTTPException(status_code=500, detail=f"创建 Dify API Key 失败: {e}")

    excel_buffer = None
    logger.debug(f"✅ 获取参数{agent_type}的模板")
    if agent_type == AgentType.WORKFLOW:
        excel_buffer = get_workflow_parameter_template(dify_api_url, api_key)
    elif agent_type == AgentType.CHATFLOW:
        excel_buffer = get_chatflow_parameter_template(dify_api_url, api_key)
    elif agent_type == AgentType.CHAT:
        excel_buffer = get_chat_parameter_template(dify_api_url, api_key)
    elif agent_type == AgentType.AGENT_CHAT:
        excel_buffer = get_agent_chat_parameter_template(dify_api_url, api_key)
    elif agent_type == AgentType.COMPLETION:
        excel_buffer = get_completion_parameter_template(dify_api_url, api_key)

    return StreamingResponse(
        excel_buffer,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": 'attachment; filename="parameter_template.xlsx"'}
    )

@router.post("/cancel_test/{uuid_str}")
async def cancel_test(uuid_str: str, db: Session = Depends(get_db)):
    record = TestRecordCRUD.get_by_uuid(db, uuid_str)
    if not record:
        raise HTTPException(status_code=404, detail="Record not found")
    logger.warning(f"取消请求已提交，任务uuid为：{uuid_str}")
    TestRecordCRUD.update_by_uuid(db, uuid_str, status=TestStatus.CANCELLED)
    return {"message": "取消请求已提交，任务将在下一次检测时终止 ✅"}

@router.post("/get_datasets_by_agent_and_bearer_paginated", status_code=status.HTTP_200_OK)
def get_datasets_by_agent_and_bearer_paginated(
    payload: dict = Body(..., example={
        "agent_id": "b35f8cb8-0f9a-4e03-87bf-9f090a3fb589",
        "bearer_token": "xxxxx",
        "page": 1,
        "page_size": 10
    })
):
    agent_id = payload.get("agent_id")
    bearer_token = payload.get("bearer_token")
    page = payload.get("page", 1)
    page_size = payload.get("page_size", 10)

    if not agent_id or not bearer_token:
        raise HTTPException(status_code=400, detail="必须提供 agent_id 和 bearer_token")

    return TestRecordCRUD.get_datasets_by_agent_and_bearer_token(agent_id, bearer_token, page, page_size)

@router.delete("/{uuid_str}/dataset", status_code=status.HTTP_200_OK)
def detach_dataset(uuid_str: str, db: Session = Depends(get_db)):
    """
    删除指定 TestRecord 的 dataset 引用（仅置空 dataset_uuid，不删除记录）
    """
    record = db.query(TestRecord).filter(
        TestRecord.uuid == uuid_str,
        TestRecord.is_deleted == False
    ).first()

    if not record:
        raise HTTPException(status_code=404, detail="TestRecord not found")

    # 如果本身就没有 dataset，则无需操作
    if record.dataset_uuid is None:
        return {"message": "Already detached", "record": record.to_dict(exclude_none=True)}

    # 调用 CRUD 层函数进行解绑
    success = TestRecordCRUD.detach_dataset(uuid_str)

    if not success:
        raise HTTPException(status_code=400, detail="Detach operation failed")

    # 重新读取记录（dataset_uuid 已为空）
    db.refresh(record)

    return {
        "message": "Dataset detached successfully",
    }


import asyncio
from sqlalchemy.ext.asyncio import AsyncEngine
from concurrent.futures import ThreadPoolExecutor
from app.core.celery_app import celery_app
from app.core.database import SessionLocal,async_engine
from app.utils.logger import logger
from app.crud.test_record_crud import TestRecordCRUD
from app.services.test_record_services import (
    test_chatflow_non_stream_pressure_wrapper,
    test_chatflow_stream_pressure_wrapper,
    test_workflow_non_stream_pressure_wrapper,
)

# 通用线程池配置
MAX_THREAD_WORKERS = 128


def run_with_async(func, *args, **kwargs):
    """
    通用封装：为 Celery 异步任务安全创建事件循环
    （防止 aiomysql 在 loop 已关闭时仍试图销毁连接）
    """
    async def _run():
        loop = asyncio.get_running_loop()
        loop.set_default_executor(ThreadPoolExecutor(max_workers=MAX_THREAD_WORKERS))
        try:
            return await func(*args, **kwargs)
        finally:
            # 🧹 安全关闭异步引擎（避免 Event loop is closed）
            if isinstance(async_engine, AsyncEngine):
                try:
                    await async_engine.dispose()
                except Exception as e:
                    print(f"⚠️ 忽略引擎销毁错误: {e}")

    # 🧠 复用全局 loop 而不是每次创建新的
    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

    # 如果 loop 已关闭，则重建一个新的
    if loop.is_closed():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

    return loop.run_until_complete(_run())


@celery_app.task(name="tasks.run_chatflow_test")
def run_chatflow_test(llm_info:dict,uuid_str: str, mode: str = "full"):
    """Celery worker 执行 Chatflow 非流式压测任务"""
    logger.info(f"🚀 [Celery] 收到 Chatflow 压测任务: {uuid_str}")

    with SessionLocal() as db:
        record = TestRecordCRUD.get_by_uuid(db, uuid_str)
        if not record:
            logger.error(f"❌ 任务记录 {uuid_str} 不存在")
            return {"status": "failed", "reason": "Record not found"}

        try:
            run_with_async(test_chatflow_non_stream_pressure_wrapper, record, llm_info, db, mode)
            logger.success(f"✅ Chatflow 压测任务完成: {uuid_str}")
            return {"uuid": uuid_str, "status": "success"}
        except Exception as e:
            logger.exception(f"❌ Chatflow 压测任务失败: {e}")
            return {"uuid": uuid_str, "status": "failed", "error": str(e)}


@celery_app.task(name="tasks.run_chatflow_stream_test")
def run_chatflow_stream_test(llm_info:dict,uuid_str: str, mode: str = "full"):
    """Celery worker 执行 Chatflow 流式压测任务"""
    logger.info(f"🚀 [Celery] 收到 Chatflow 流式压测任务: {uuid_str}")

    with SessionLocal() as db:
        record = TestRecordCRUD.get_by_uuid(db, uuid_str)
        if not record:
            logger.error(f"❌ 任务记录 {uuid_str} 不存在")
            return {"status": "failed", "reason": "Record not found"}

        try:
            run_with_async(test_chatflow_stream_pressure_wrapper, record, llm_info, db, mode)
            logger.success(f"✅ Chatflow Stream 压测任务完成: {uuid_str}")
            return {"uuid": uuid_str, "status": "success"}
        except Exception as e:
            logger.exception(f"❌ Chatflow Stream 压测任务失败: {e}")
            return {"uuid": uuid_str, "status": "failed", "error": str(e)}


@celery_app.task(name="tasks.run_workflow_test")
def run_workflow_test(llm_info:dict,uuid_str: str, mode: str = "full"):
    """Celery worker 执行 Workflow 压测任务"""
    logger.info(f"🚀 [Celery] 收到 Workflow 压测任务: {uuid_str}")

    with SessionLocal() as db:
        record = TestRecordCRUD.get_by_uuid(db, uuid_str)
        if not record:
            logger.error(f"❌ 任务记录 {uuid_str} 不存在")
            return {"status": "failed", "reason": "Record not found"}

        try:
            run_with_async(test_workflow_non_stream_pressure_wrapper, record, llm_info, db, mode)
            logger.success(f"✅ Workflow 压测任务完成: {uuid_str}")
            return {"uuid": uuid_str, "status": "success"}
        except Exception as e:
            logger.exception(f"❌ Workflow 压测任务失败: {e}")
            return {"uuid": uuid_str, "status": "failed", "error": str(e)}

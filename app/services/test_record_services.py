from pathlib import Path
import pandas as pd
import numpy as np
import asyncio
import aiohttp
from fastapi import Request
from sqlalchemy.orm import Session

from app.utils.pressure_test_util import (
    single_test_chatflow_non_stream_pressure,
    single_test_workflow_non_stream_pressure,
    validate_entry,
    get_agent_input_para_dict,
)
from app.core.database import SessionLocal
from app.utils.logger import logger
from app.models.test_record import TestRecord, TestStatus
from app.crud.test_record_crud import TestRecordCRUD
from app.crud.single_run_result_crud import SingleRunResultCRUD


# ==========================================================
# 通用工具函数
# ==========================================================

def align_dify_input_types(df_data: pd.DataFrame, df_schema: pd.DataFrame) -> pd.DataFrame:
    """根据 Dify 参数 schema 对用户上传的数据进行格式转换。"""
    df_result = df_data.copy()
    for _, row in df_schema.iterrows():
        var = row["variable"]
        typ = row["type"]
        options = row["options"]
        required = row["required"]

        if var not in df_result.columns:
            logger.warning(f"⚠️ Missing column in data: {var}")
            df_result[var] = np.nan
            continue

        if typ in ("text-input", "paragraph"):
            df_result[var] = df_result[var].astype(str).fillna("")
        elif typ == "number":
            df_result[var] = pd.to_numeric(df_result[var], errors="coerce")
            if required and df_result[var].isna().any():
                logger.error(f"❌ Required numeric field '{var}' has invalid values")
        elif typ == "select":
            df_result[var] = df_result[var].astype(str)
            valid_opts = set(options)
            if valid_opts:
                df_result[var] = df_result[var].apply(lambda x: x if x in valid_opts else None)
                if required and df_result[var].isna().any():
                    logger.warning(f"⚠️ Invalid choice in '{var}'")
        else:
            logger.info(f"ℹ️ Unknown type {typ}, left unchanged")

    return df_result


# ==========================================================
# 同步检测函数（替代异步检测）
# ==========================================================

def check_cancelled_sync(_, uuid_str: str):
    # 🔥 新建独立 session，防止缓存污染
    with SessionLocal() as local_db:
        record = local_db.query(TestRecord.status).filter(TestRecord.uuid == uuid_str).first()
        if record and record[0] == TestStatus.CANCELLED:
            logger.warning(f"🚫 检测到任务 {uuid_str} 已被取消，中止执行。")
            raise asyncio.CancelledError()


# ==========================================================
# Chatflow 异步执行器（同步检测取消）
# ==========================================================

async def run_chatflow_tests_async(
    df,
    input_uuid: str,
    input_dify_url: str,
    input_dify_api_key: str,
    input_dify_username: str,
    input_judge_prompt: str,
    llm,
    db: Session,
    concurrency: int = 10,
):
    """异步限并发执行 Chatflow 测试任务，检测到取消时立即终止所有任务。"""
    semaphore = asyncio.Semaphore(concurrency)
    all_results = []

    async def _run_single(index, row, session):
        check_cancelled_sync(db, input_uuid)
        row_dict = row.to_dict()
        input_query = row_dict.get("query", "")
        async with semaphore:
            try:
                logger.debug(f"开始执行第 {index + 1} 行 Chatflow 测试")
                result = await asyncio.to_thread(
                    single_test_chatflow_non_stream_pressure,
                    input_dify_url=input_dify_url,
                    input_dify_api_key=input_dify_api_key,
                    input_query=input_query,
                    input_dify_username=input_dify_username,
                    input_data_dict=row_dict,
                    input_judge_prompt=input_judge_prompt,
                    llm=llm,
                )

                check_cancelled_sync(db, input_uuid)
                await asyncio.to_thread(TestRecordCRUD.increment_success_count, input_uuid)
                single_run_data = {
                    "chatflow_query": input_query,
                    "test_params": row_dict,
                    "input_task_uuid": input_uuid,
                    "input_time_consumption": result["time_consumption"],
                    "input_score": result["score"],
                    "input_tps": result["TPS"],
                    "input_generated_answer": result["generated_answer"],
                }
                await asyncio.to_thread(SingleRunResultCRUD.create, **single_run_data)
                logger.success(f"✅ [Row {index + 1}] 测试完成")
                return result

            except asyncio.CancelledError:
                logger.warning(f"❌ [Row {index + 1}] Chatflow 测试被取消")
                raise
            except Exception as e:
                await asyncio.to_thread(TestRecordCRUD.increment_failure_count, input_uuid)
                logger.error(f"❌ [Row {index + 1}] Chatflow 出错: {e}")
                return {"index": index, "error": str(e)}

    logger.info(f"🚀 启动 Chatflow 异步测试，共 {len(df)} 条记录，最大并发={concurrency}")

    try:
        async with aiohttp.ClientSession() as session:
            tasks = [_run_single(i, row, session) for i, row in df.iterrows()]
            pending = set(asyncio.create_task(t) for t in tasks)

            while pending:
                done, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)

                for task in done:
                    try:
                        check_cancelled_sync(db, input_uuid)
                        result = task.result()
                        all_results.append(result)
                    except asyncio.CancelledError:
                        logger.warning(f"🚫 检测到任务 {input_uuid} 被取消，取消所有剩余任务")
                        for p in pending:
                            p.cancel()
                        raise
                    except Exception as e:
                        logger.error(f"❌ 子任务异常: {e}")

    except asyncio.CancelledError:
        logger.warning(f"🛑 Chatflow 测试任务 {input_uuid} 已终止")
        raise

    logger.info(f"🏁 Chatflow 异步测试完成，共 {len(all_results)} 条结果")
    return all_results


# ==========================================================
# Workflow 异步执行器（同步检测取消）
# ==========================================================

async def run_workflow_tests_async(
    df,
    input_uuid: str,
    input_dify_url: str,
    input_dify_api_key: str,
    input_dify_username: str,
    input_judge_prompt: str,
    llm,
    db: Session,
    concurrency: int = 10,
):
    """异步限并发执行 Workflow 测试任务，检测到取消后立即终止所有任务。"""
    semaphore = asyncio.Semaphore(concurrency)
    all_results = []

    async def _run_single(index, row, session):
        # 每个子任务开始前检测
        check_cancelled_sync(db, input_uuid)
        row_dict = row.to_dict()

        async with semaphore:
            try:
                logger.debug(f"开始执行第 {index + 1} 行 Workflow 测试")
                result = await asyncio.to_thread(
                    single_test_workflow_non_stream_pressure,
                    input_dify_url=input_dify_url,
                    input_dify_api_key=input_dify_api_key,
                    input_dify_username=input_dify_username,
                    input_data_dict=row_dict,
                    input_judge_prompt=input_judge_prompt,
                    llm=llm,
                )

                # 每个任务完成后再次检测取消状态
                check_cancelled_sync(db, input_uuid)

                await asyncio.to_thread(TestRecordCRUD.increment_success_count, input_uuid)

                single_run_data = {
                    "chatflow_query": "",
                    "test_params": row_dict,
                    "input_task_uuid": input_uuid,
                    "input_time_consumption": result["time_consumption"],
                    "input_score": result["score"],
                    "input_tps": result["TPS"],
                    "input_generated_answer": result["generated_answer"],
                }
                await asyncio.to_thread(SingleRunResultCRUD.create, **single_run_data)

                logger.success(f"✅ [Row {index + 1}] Workflow 测试完成")
                return result

            except asyncio.CancelledError:
                logger.warning(f"❌ [Row {index + 1}] Workflow 测试被取消")
                raise

            except Exception as e:
                await asyncio.to_thread(TestRecordCRUD.increment_failure_count, input_uuid)
                logger.error(f"❌ [Row {index + 1}] Workflow 出错: {e}")
                return {"index": index, "error": str(e)}

    logger.info(f"🚀 启动 Workflow 异步测试，共 {len(df)} 条记录，最大并发={concurrency}")

    try:
        async with aiohttp.ClientSession() as session:
            # ✅ 创建所有任务并放入 pending 集合
            tasks = [_run_single(i, row, session) for i, row in df.iterrows()]
            pending = set(asyncio.create_task(t) for t in tasks)

            while pending:
                # ✅ 每次等待最先完成的任务
                done, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)

                for task in done:
                    try:
                        # 🔥 检测是否取消（同步 DB 检查）
                        check_cancelled_sync(db, input_uuid)
                        result = task.result()
                        all_results.append(result)
                    except asyncio.CancelledError:
                        logger.warning(f"🚫 检测到任务 {input_uuid} 已被取消，终止所有未完成 Workflow 任务")
                        # ✅ 取消所有未完成的协程任务
                        for p in pending:
                            if not p.done():
                                p.cancel()
                        # ✅ 等待所有任务安全退出
                        await asyncio.gather(*pending, return_exceptions=True)
                        raise
                    except Exception as e:
                        logger.error(f"❌ 子任务异常: {e}")

    except asyncio.CancelledError:
        logger.warning(f"🛑 Workflow 测试任务 {input_uuid} 被用户取消，中止执行")
        raise

    logger.info(f"🏁 Workflow 异步测试完成，共 {len(all_results)} 条结果")
    return all_results


# ==========================================================
# Wrapper 层（保持逻辑一致）
# ==========================================================

async def test_chatflow_non_stream_pressure_wrapper(
    testrecord: TestRecord,
    request: Request,
    db: Session,
    mode: str,
):
    """Chatflow 压测任务包装器（同步检测取消）"""

    input_dify_url = testrecord.dify_api_url
    input_dify_api_key = testrecord.dify_api_key
    input_username = testrecord.dify_username
    input_dify_test_file = Path("uploads/" + testrecord.filename).resolve()
    input_concurrency = testrecord.concurrency
    input_judge_prompt = testrecord.judge_prompt

    # 读取文件
    if input_dify_test_file.suffix == ".csv":
        df = await asyncio.to_thread(pd.read_csv, input_dify_test_file)
    elif input_dify_test_file.suffix == ".xlsx":
        df = await asyncio.to_thread(pd.read_excel, input_dify_test_file, engine="openpyxl")
    else:
        raise ValueError("Unsupported file type. Only .csv and .xlsx are supported.")

    if mode == "experiment":
        df = df.head(3)

    # 获取 schema 并验证
    para_df = await asyncio.to_thread(get_agent_input_para_dict, input_dify_url, input_dify_api_key)
    df = align_dify_input_types(df, para_df)
    for _, row in df.iterrows():
        row_dict = row.to_dict()
        error = validate_entry(row_dict, para_df)
        if error:
            return {"error": error}

    llm = request.session.get("llm")
    TestRecordCRUD.update_by_uuid(db, testrecord.uuid, status=TestStatus.RUNNING)

    try:
        results = await run_chatflow_tests_async(
            df,
            input_uuid=testrecord.uuid,
            input_dify_url=input_dify_url,
            input_dify_api_key=input_dify_api_key,
            input_dify_username=input_username,
            concurrency=input_concurrency,
            input_judge_prompt=input_judge_prompt,
            llm=llm,
            db=db,
        )
    except asyncio.CancelledError:
        TestRecordCRUD.update_by_uuid(db, testrecord.uuid, status=TestStatus.CANCELLED)
        logger.warning(f"任务 {testrecord.uuid} 被取消")
        return {"cancelled": True}

    avg_time = sum(ele.get("time_consumption") for ele in results) / len(results)
    total_time = sum(ele.get("time_consumption") for ele in results)
    avg_token = sum(ele.get("token_num") for ele in results) / len(results)
    avg_TPS = sum(ele.get("TPS") for ele in results) / len(results)
    avg_score = sum(ele.get("score") for ele in results) / len(results)

    result_dict = {
        "avg_time_consumption": avg_time,
        "avg_token_num": avg_token,
        "avg_TPS": avg_TPS,
        "avg_score": avg_score,
    }

    logger.success(f"Chatflow 测试结果: {result_dict}, 模式: {mode}")
    update_data_dict = {
        "status": TestStatus.EXPERIMENT if mode == "experiment" else TestStatus.SUCCESS,
        "duration": total_time,
        "result": result_dict,
    }
    TestRecordCRUD.update_by_uuid(db, testrecord.uuid, **update_data_dict)
    return result_dict


async def test_workflow_non_stream_pressure_wrapper(
    testrecord: TestRecord,
    request: Request,
    db: Session,
    mode: str,
):
    """Workflow 压测任务包装器（同步检测取消）"""

    input_dify_url = testrecord.dify_api_url
    input_dify_api_key = testrecord.dify_api_key
    input_username = testrecord.dify_username
    input_dify_test_file = Path("uploads/" + testrecord.filename).resolve()
    input_concurrency = testrecord.concurrency
    input_judge_prompt = testrecord.judge_prompt

    if input_dify_test_file.suffix == ".csv":
        df = await asyncio.to_thread(pd.read_csv, input_dify_test_file)
    elif input_dify_test_file.suffix == ".xlsx":
        df = await asyncio.to_thread(pd.read_excel, input_dify_test_file, engine="openpyxl")
    else:
        raise ValueError("Unsupported file type. Only .csv and .xlsx are supported.")

    if mode == "experiment":
        df = df.head(3)

    para_df = await asyncio.to_thread(get_agent_input_para_dict, input_dify_url, input_dify_api_key)
    df = align_dify_input_types(df, para_df)
    for _, row in df.iterrows():
        row_dict = row.to_dict()
        error = validate_entry(row_dict, para_df)
        if error:
            return {"error": error}

    llm = request.session.get("llm")
    TestRecordCRUD.update_by_uuid(db, testrecord.uuid, status=TestStatus.RUNNING)

    try:
        results = await run_workflow_tests_async(
            df,
            input_uuid=testrecord.uuid,
            input_dify_url=input_dify_url,
            input_dify_api_key=input_dify_api_key,
            input_dify_username=input_username,
            concurrency=input_concurrency,
            input_judge_prompt=input_judge_prompt,
            llm=llm,
            db=db,
        )
    except asyncio.CancelledError:
        TestRecordCRUD.update_by_uuid(db, testrecord.uuid, status=TestStatus.CANCELLED)
        logger.warning(f"任务 {testrecord.uuid} 被取消")
        return {"cancelled": True}

    avg_time = sum(ele.get("time_consumption") for ele in results) / len(results)
    total_time = sum(ele.get("time_consumption") for ele in results)
    avg_token = sum(ele.get("token_num") for ele in results) / len(results)
    avg_TPS = sum(ele.get("TPS") for ele in results) / len(results)
    avg_score = sum(ele.get("score") for ele in results) / len(results)

    result_dict = {
        "avg_time_consumption": avg_time,
        "avg_token_num": avg_token,
        "avg_TPS": avg_TPS,
        "avg_score": avg_score,
    }

    logger.success(f"Workflow 测试结果: {result_dict}, 模式: {mode}")
    update_data_dict = {
        "status": TestStatus.EXPERIMENT if mode == "experiment" else TestStatus.SUCCESS,
        "duration": total_time,
        "result": result_dict,
    }
    TestRecordCRUD.update_by_uuid(db, testrecord.uuid, **update_data_dict)
    return result_dict

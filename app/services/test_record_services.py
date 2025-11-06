from pathlib import Path
import pandas as pd
import numpy as np
import asyncio
from concurrent.futures import ThreadPoolExecutor
executor = ThreadPoolExecutor(max_workers=128)
asyncio.get_event_loop().set_default_executor(executor)
import aiohttp
from fastapi import HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import select
import os
import time

from app.utils.pressure_test_util import (
    single_test_chatflow_non_stream_pressure,
    single_test_chatflow_stream_pressure,
    single_test_workflow_non_stream_pressure,
    validate_entry,
    get_agent_input_para_dict,
    download_from_tos
)
from app.core.database import SessionLocal,AsyncSessionLocal
from app.utils.logger import logger
from app.models.test_record import TestRecord, TestStatus
from app.crud.test_record_crud import TestRecordCRUD
from app.crud.single_run_result_crud import SingleRunResultCRUD


# ==========================================================
# 通用工具函数
# ==========================================================

async def validate_dataset_rows_async(
    df,
    para_df,
    db,
    testrecord,
    validate_entry,
    max_concurrency: int = 20,  # 并发控制
):
    """
    高性能异步验证数据集行内容是否合法：
    - 并发执行行级校验，使用 asyncio.Semaphore 控制最大并发量
    - 检查 query / ref_answer 是否为空
    - 调用 validate_entry 验证字段格式
    - 异常时更新数据库状态为 FAILED 并抛出 HTTPException
    """
    semaphore = asyncio.Semaphore(max_concurrency)
    tasks = []

    async def validate_single_row(row_idx: int, row_dict: dict):
        """单行校验逻辑"""
        async with semaphore:
            try:
                # ✅ 检查 query 字段
                if "query" in row_dict:
                    query_val = row_dict.get("query")
                    if query_val is np.nan or not str(query_val).strip():
                        logger.error(f"❌ [Row {row_idx}] 输入验证失败: query 字段为空")
                        await asyncio.to_thread(
                            TestRecordCRUD.update_by_uuid,
                            db,
                            testrecord.uuid,
                            status=TestStatus.FAILED,
                        )
                        raise HTTPException(status_code=400, detail="输入验证失败: query 字段为空")

                # ✅ 检查 ref_answer 字段
                if "ref_answer" in row_dict:
                    ref_val = row_dict.get("ref_answer")
                    if ref_val is np.nan or not str(ref_val).strip():
                        logger.error(f"❌ [Row {row_idx}] 输入验证失败: ref_answer 字段为空")
                        await asyncio.to_thread(
                            TestRecordCRUD.update_by_uuid,
                            db,
                            testrecord.uuid,
                            status=TestStatus.FAILED,
                        )
                        raise HTTPException(status_code=400, detail="输入验证失败: ref_answer 字段为空")

                # ✅ 调用业务逻辑验证
                error = await asyncio.to_thread(validate_entry, row_dict, para_df)
                if error:
                    logger.error(f"❌ [Row {row_idx}] 输入验证失败: {error}")
                    await asyncio.to_thread(
                        TestRecordCRUD.update_by_uuid,
                        db,
                        testrecord.uuid,
                        status=TestStatus.FAILED,
                    )
                    raise HTTPException(status_code=400, detail=f"输入验证失败: {error}")

            except Exception as e:
                logger.error(f"❌ [Row {row_idx}] 异常: {e}")
                await asyncio.to_thread(
                    TestRecordCRUD.update_by_uuid,
                    db,
                    testrecord.uuid,
                    status=TestStatus.FAILED,
                )
                raise HTTPException(status_code=400, detail=f"输入验证失败: {e}")

    # ✅ 使用 itertuples 提升迭代速度（比 iterrows 快 10 倍）
    for i, row in enumerate(df.itertuples(index=False), start=1):
        row_dict = row._asdict()
        tasks.append(validate_single_row(i, row_dict))

    # ✅ 并发执行所有验证任务
    try:
        await asyncio.gather(*tasks)
        logger.success(f"✅ 数据验证完成，共 {len(tasks)} 行，未发现错误")
    except HTTPException as e:
        logger.warning(f"🛑 验证中断: {e.detail}")
        raise

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

async def check_cancelled_async(uuid_str: str):
    """🔍 异步检测任务是否已被取消"""
    async with AsyncSessionLocal() as session:
        stmt = select(TestRecord.status).where(TestRecord.uuid == uuid_str)
        result = await session.execute(stmt)
        record = result.first()

        if record and record[0] == TestStatus.CANCELLED:
            logger.warning(f"🚫 检测到任务 {uuid_str} 已被取消，中止执行。")
            raise asyncio.CancelledError()

def check_cancelled_sync(uuid_str: str):
    """同步检测任务是否已被取消（线程安全、高性能）"""
    with SessionLocal() as session:
        stmt = select(TestRecord.status).where(TestRecord.uuid == uuid_str)
        result = session.execute(stmt).first()

        if result and result[0] == TestStatus.CANCELLED:
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
    input_agent_type: str,
    llm,
    db: Session,
    concurrency: int = 10,
):
    """异步限并发执行 Chatflow 测试任务，检测到取消时立即终止所有任务。"""
    semaphore = asyncio.Semaphore(concurrency)
    all_results = []

    async def _run_single(index, row, session):
        await asyncio.to_thread(check_cancelled_sync, input_uuid)
        row_dict = row.to_dict()
        input_query = row_dict.get("query", "")
        async with semaphore:
            try:
                logger.debug(f"开始执行第 {index + 1} 行 Chatflow 测试")

                result = await single_test_chatflow_non_stream_pressure(
                    async_request_session=session,  # ⚠️ 如果外层已有 aiohttp.ClientSession
                    input_agent_type=input_agent_type,
                    input_dify_url=input_dify_url,
                    input_dify_api_key=input_dify_api_key,
                    input_query=input_query,
                    input_dify_username=input_dify_username,
                    input_data_dict=row_dict,
                    input_judge_prompt=input_judge_prompt,
                    llm=llm,
                )
                if result["error_info"]:
                    raise Exception

                await asyncio.to_thread(check_cancelled_sync, input_uuid)
                await TestRecordCRUD.increment_success_count(input_uuid)
                single_run_data = {
                    "chatflow_query": input_query,
                    "test_params": row_dict,
                    "input_task_uuid": input_uuid,
                    "input_time_consumption": result["time_consumption"],
                    "input_score": result["score"],
                    "input_tps": result["TPS"],
                    "input_generated_answer": result["generated_answer"],
                }
                await SingleRunResultCRUD.create(**single_run_data)

                logger.success(f"✅ [Row {index + 1}] 测试完成,input_query:{input_query}, data:{single_run_data}")
                return result

            except asyncio.CancelledError:
                logger.warning(f"❌ [Row {index + 1}] Chatflow 测试被取消")
                raise
            except Exception as e:
                await TestRecordCRUD.increment_failure_count(input_uuid)
                logger.error(f"❌ [Row {index + 1}] Chatflow 出错: {e}")
                return {
                    "error_info": str(e),
                    "generated_answer": "",
                    "score": 0,
                    "TPS": 0,
                    "time_consumption": 0,
                }

    logger.info(f"🚀 启动 Chatflow 异步测试，共 {len(df)} 条记录，最大并发={concurrency}")

    try:
        async with aiohttp.ClientSession() as session:
            tasks = [_run_single(i, row, session) for i, row in df.iterrows()]
            pending = set(asyncio.create_task(t) for t in tasks)

            while pending:
                done, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)

                for task in done:
                    try:
                        await asyncio.to_thread(check_cancelled_sync, input_uuid)
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

async def run_chatflow_stream_tests_async(
    df,
    input_uuid: str,
    input_dify_url: str,
    input_dify_api_key: str,
    input_dify_username: str,
    input_judge_prompt: str,
    input_agent_type: str,
    llm,
    db: Session,
    concurrency: int = 10,
):
    """异步限并发执行 Chatflow 测试任务，检测到取消时立即终止所有任务。"""
    semaphore = asyncio.Semaphore(concurrency)
    all_results = []

    async def _run_single(index, row, session):
        time.sleep(1)
        logger.warning(f"开始执行第 {index + 1} 行 Chatflow Stream测试前，睡眠 1 秒，防止测试过快")
        await asyncio.to_thread(check_cancelled_sync, input_uuid)
        row_dict = row.to_dict()
        input_query = row_dict.get("query", "")
        async with semaphore:
            try:
                logger.debug(f"开始执行第 {index + 1} 行 Chatflow 测试")
                result = await single_test_chatflow_stream_pressure(
                    async_request_session=session,  # ✅ 一定传入共用的 aiohttp.ClientSession
                    input_agent_type=input_agent_type,
                    input_dify_url=input_dify_url,
                    input_dify_api_key=input_dify_api_key,
                    input_query=input_query,
                    input_dify_username=input_dify_username,
                    input_data_dict=row_dict,
                    input_judge_prompt=input_judge_prompt,
                    llm=llm,
                )

                if result["error_info"]:
                    raise Exception

                await asyncio.to_thread(check_cancelled_sync, input_uuid)
                await TestRecordCRUD.increment_success_count(input_uuid)
                single_run_data = {
                    "chatflow_query": input_query,
                    "test_params": row_dict,
                    "input_task_uuid": input_uuid,
                    "input_time_consumption": result["time_consumption"],
                    "input_score": result["score"],
                    "input_tps": result["TPS"],
                    "input_generated_answer": result["generated_answer"],
                }
                await SingleRunResultCRUD.create(**single_run_data)

                logger.success(f"✅ [Row {index + 1}] 测试完成,input_query:{input_query}, data:{single_run_data}")
                return result

            except asyncio.CancelledError:
                logger.warning(f"❌ [Row {index + 1}] Chatflow 测试被取消")
                raise
            except Exception as e:
                await TestRecordCRUD.increment_failure_count(input_uuid)
                logger.error(f"❌ [Row {index + 1}] Chatflow 出错: {e}")
                return {
                    "error_info": str(e),
                    "generated_answer": "",
                    "score": 0,
                    "TPS": 0,
                    "time_consumption": 0,
                }

    logger.info(f"🚀 启动 Chatflow 异步测试，共 {len(df)} 条记录，最大并发={concurrency}")

    try:
        async with aiohttp.ClientSession() as session:
            tasks = [_run_single(i, row, session) for i, row in df.iterrows()]
            pending = set(asyncio.create_task(t) for t in tasks)

            while pending:
                done, pending = await asyncio.wait(pending, return_when=asyncio.FIRST_COMPLETED)

                for task in done:
                    try:
                        await asyncio.to_thread(check_cancelled_sync, input_uuid)
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
        time.sleep(1)
        logger.warning(f"开始执行第 {index + 1} 行 Workflow 测试前，睡眠 1 秒，防止测试过快")
        await asyncio.to_thread(check_cancelled_sync, input_uuid)
        row_dict = row.to_dict()

        async with semaphore:
            try:
                logger.debug(f"开始执行第 {index + 1} 行 Workflow 测试")
                result = await single_test_workflow_non_stream_pressure(
                    async_request_session=session,  # ⚠️ 传入共用的 aiohttp.ClientSession
                    input_dify_url=input_dify_url,
                    input_dify_api_key=input_dify_api_key,
                    input_dify_username=input_dify_username,
                    input_data_dict=row_dict,
                    input_judge_prompt=input_judge_prompt,
                    llm=llm,
                )
                if result["error_info"]:
                    raise Exception
                # 每个任务完成后再次检测取消状态
                await asyncio.to_thread(check_cancelled_sync, input_uuid)

                await TestRecordCRUD.increment_success_count(input_uuid)

                single_run_data = {
                    "chatflow_query": "",
                    "test_params": row_dict,
                    "input_task_uuid": input_uuid,
                    "input_time_consumption": result["time_consumption"],
                    "input_score": result["score"],
                    "input_tps": result["TPS"],
                    "input_generated_answer": result["generated_answer"],
                }
                logger.warning(f"🚀 [Row {index + 1}] 测试完成,input_query:, data:{single_run_data}")
                await SingleRunResultCRUD.create(**single_run_data)

                logger.success(f"✅ [Row {index + 1}] Workflow 测试完成")
                return result

            except asyncio.CancelledError:
                logger.warning(f"❌ [Row {index + 1}] Workflow 测试被取消")
                raise

            except Exception as e:
                await TestRecordCRUD.increment_failure_count(input_uuid)
                logger.error(f"❌ [Row {index + 1}] Workflow 出错: {e}")
                return {
                    "error_info": str(e),
                    "generated_answer": "",
                    "score": 0,
                    "TPS": 0,
                    "time_consumption": 0,
                }

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
                        await asyncio.to_thread(check_cancelled_sync, input_uuid)
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

async def test_chatflow_stream_pressure_wrapper(
        testrecord,
        llm_info: dict,
        db,
        mode: str,
):
    # 基本参数
    input_dify_url = testrecord.dify_api_url
    input_dify_api_key = testrecord.dify_api_key
    input_username = testrecord.dify_username
    input_concurrency = testrecord.concurrency
    input_judge_prompt = testrecord.judge_prompt
    input_agent_type = testrecord.agent_type
    dataset_key = testrecord.dataset_tos_key

    if testrecord.status == TestStatus.CANCELLED:
        logger.warning(f"任务 {testrecord.uuid} 已被取消")
        return    {
        "avg_time_consumption": 0,
        "avg_token_num": 0,
        "avg_TPS": 0,
        "avg_score": 0,
        }

    if not dataset_key:
        raise ValueError("❌ 当前记录缺少 dataset_tos_key，无法从 TOS 下载数据集。")

    # 1️⃣ 下载数据集到临时文件
    tmp_dir = Path("uploads/tmp")
    tmp_dir.mkdir(parents=True, exist_ok=True)
    tmp_path = tmp_dir / f"{testrecord.uuid}_{os.path.basename(dataset_key)}"

    try:
        logger.info(f"⬇️ 正在从 TOS 下载数据集: {dataset_key} → {tmp_path}")
        await asyncio.to_thread(download_from_tos, dataset_key, tmp_path)
        logger.success(f"✅ 数据集下载完成: {tmp_path}")
    except Exception as e:
        logger.error(f"❌ 从 TOS 下载失败: {e}")
        raise RuntimeError(f"下载 TOS 数据集失败: {e}")

    # 2️⃣ 读取文件内容
    try:
        suffix = tmp_path.suffix.lower()
        if suffix == ".csv":
            df = await asyncio.to_thread(pd.read_csv, tmp_path)
        elif suffix in [".xls", ".xlsx"]:
            df = await asyncio.to_thread(pd.read_excel, tmp_path, engine="openpyxl")
        else:
            raise ValueError(f"Unsupported file type: {suffix}")

        if mode == "experiment":
            df = df.head(3)

        logger.info(f"✅ 数据集 {tmp_path.name} 读取成功，共 {len(df)} 行")
    except Exception as e:
        logger.error(f"❌ 文件读取失败: {e}")
        raise HTTPException(status_code=400, detail=f"无法读取文件内容: {e}")

    # 3️⃣ 验证输入
    para_df = await asyncio.to_thread(get_agent_input_para_dict, input_dify_url, input_dify_api_key)
    df = align_dify_input_types(df, para_df)
    await validate_dataset_rows_async(
        df=df,
        para_df=para_df,
        db=db,
        testrecord=testrecord,
        validate_entry=validate_entry
    )

    # 4️⃣ 更新状态为 RUNNING
    TestRecordCRUD.update_by_uuid(db, testrecord.uuid, status=TestStatus.RUNNING)

    llm = llm_info

    try:
        results = await run_chatflow_stream_tests_async(
            df,
            input_uuid=testrecord.uuid,
            input_dify_url=input_dify_url,
            input_dify_api_key=input_dify_api_key,
            input_dify_username=input_username,
            input_agent_type=input_agent_type,
            concurrency=input_concurrency,
            input_judge_prompt=input_judge_prompt,
            llm=llm,
            db=db,
        )
    except asyncio.CancelledError:
        TestRecordCRUD.update_by_uuid(db, testrecord.uuid, status=TestStatus.CANCELLED)
        logger.warning(f"任务 {testrecord.uuid} 被取消")
        return {"cancelled": True}
    finally:
        # 5️⃣ 清理临时文件
        try:
            if tmp_path.exists():
                tmp_path.unlink()
                logger.info(f"🧹 已清理临时文件: {tmp_path}")
        except Exception as e:
            logger.warning(f"⚠️ 清理临时文件失败: {e}")

    # 6️⃣ 计算结果
    avg_time = sum(ele.get("time_consumption") for ele in results) / len(results)
    total_time = sum(ele.get("time_consumption") for ele in results)
    avg_token = sum(ele.get("token_num") for ele in results) / len(results)
    avg_TPS = sum(ele.get("TPS") for ele in results) / len(results)
    avg_score = sum(float(ele.get("score")) for ele in results) / len(results)

    result_dict = {
        "avg_time_consumption": avg_time,
        "avg_token_num": avg_token,
        "avg_TPS": avg_TPS,
        "avg_score": avg_score,
    }

    # 7️⃣ 更新数据库状态
    TestRecordCRUD.update_by_uuid(
        db,
        testrecord.uuid,
        status=TestStatus.EXPERIMENT if mode == "experiment" else TestStatus.SUCCESS,
        duration=total_time,
        result=result_dict,
    )

    logger.success(f"✅ Chatflow 压测完成: {result_dict} (mode={mode})")
    return result_dict



async def test_chatflow_non_stream_pressure_wrapper(
    testrecord,
    llm_info: dict,
    db,
    mode: str,
):
    """
    Chatflow 压测任务包装器（从 TOS 下载数据集、执行后清理临时文件）
    """
    # 基本参数
    input_dify_url = testrecord.dify_api_url
    input_dify_api_key = testrecord.dify_api_key
    input_username = testrecord.dify_username
    input_concurrency = testrecord.concurrency
    input_judge_prompt = testrecord.judge_prompt
    input_agent_type = testrecord.agent_type
    dataset_key = testrecord.dataset_tos_key

    if testrecord.status == TestStatus.CANCELLED:
        logger.warning(f"任务 {testrecord.uuid} 已被取消")
        return    {
        "avg_time_consumption": 0,
        "avg_token_num": 0,
        "avg_TPS": 0,
        "avg_score": 0,
        }

    if not dataset_key:
        raise ValueError("❌ 当前记录缺少 dataset_tos_key，无法从 TOS 下载数据集。")

    # 1️⃣ 下载数据集到临时文件
    tmp_dir = Path("uploads/tmp")
    tmp_dir.mkdir(parents=True, exist_ok=True)
    tmp_path = tmp_dir / f"{testrecord.uuid}_{os.path.basename(dataset_key)}"

    try:
        logger.info(f"⬇️ 正在从 TOS 下载数据集: {dataset_key} → {tmp_path}")
        await asyncio.to_thread(download_from_tos, dataset_key, tmp_path)
        logger.success(f"✅ 数据集下载完成: {tmp_path}")
    except Exception as e:
        logger.error(f"❌ 从 TOS 下载失败: {e}")
        raise RuntimeError(f"下载 TOS 数据集失败: {e}")

    # 2️⃣ 读取文件内容
    try:
        suffix = tmp_path.suffix.lower()
        if suffix == ".csv":
            df = await asyncio.to_thread(pd.read_csv, tmp_path)
        elif suffix in [".xls", ".xlsx"]:
            df = await asyncio.to_thread(pd.read_excel, tmp_path, engine="openpyxl")
        else:
            raise ValueError(f"Unsupported file type: {suffix}")

        if mode == "experiment":
            df = df.head(3)

        logger.info(f"✅ 数据集 {tmp_path.name} 读取成功，共 {len(df)} 行")
    except Exception as e:
        logger.error(f"❌ 文件读取失败: {e}")
        raise HTTPException(status_code=400, detail=f"无法读取文件内容: {e}")

    # 3️⃣ 验证输入
    para_df = await asyncio.to_thread(get_agent_input_para_dict, input_dify_url, input_dify_api_key)
    df = align_dify_input_types(df, para_df)

    await validate_dataset_rows_async(
        df=df,
        para_df=para_df,
        db=db,
        testrecord=testrecord,
        validate_entry=validate_entry,
    )

    # 4️⃣ 更新状态为 RUNNING
    TestRecordCRUD.update_by_uuid(db, testrecord.uuid, status=TestStatus.RUNNING)

    llm = llm_info

    try:
        results = await run_chatflow_tests_async(
            df,
            input_uuid=testrecord.uuid,
            input_dify_url=input_dify_url,
            input_dify_api_key=input_dify_api_key,
            input_dify_username=input_username,
            input_agent_type=input_agent_type,
            concurrency=input_concurrency,
            input_judge_prompt=input_judge_prompt,
            llm=llm,
            db=db,
        )
    except asyncio.CancelledError:
        TestRecordCRUD.update_by_uuid(db, testrecord.uuid, status=TestStatus.CANCELLED)
        logger.warning(f"任务 {testrecord.uuid} 被取消")
        return {"cancelled": True}
    finally:
        # 5️⃣ 清理临时文件
        try:
            if tmp_path.exists():
                tmp_path.unlink()
                logger.info(f"🧹 已清理临时文件: {tmp_path}")
        except Exception as e:
            logger.warning(f"⚠️ 清理临时文件失败: {e}")

    logger.warning(f"计算结果: {results}")

    # 6️⃣ 计算结果
    avg_time = sum(ele.get("time_consumption",0) for ele in results) / len(results)
    total_time = sum(ele.get("time_consumption",0) for ele in results)
    avg_token = sum(ele.get("token_num",0) for ele in results) / len(results)
    avg_TPS = sum(ele.get("TPS",0) for ele in results) / len(results)
    avg_score = sum(float(ele.get("score",0)) for ele in results) / len(results)

    result_dict = {
        "avg_time_consumption": avg_time,
        "avg_token_num": avg_token,
        "avg_TPS": avg_TPS,
        "avg_score": avg_score,
    }

    # 7️⃣ 更新数据库状态
    TestRecordCRUD.update_by_uuid(
        db,
        testrecord.uuid,
        status=TestStatus.EXPERIMENT if mode == "experiment" else TestStatus.SUCCESS,
        duration=total_time,
        result=result_dict,
    )

    logger.success(f"✅ Chatflow 压测完成: {result_dict} (mode={mode})")
    return result_dict


async def test_workflow_non_stream_pressure_wrapper(
    testrecord,
    llm_info: dict,
    db,
    mode: str,
):
    """Workflow 压测任务包装器（从 TOS 下载数据集并在完成后清理临时文件）"""

    # 1️⃣ 读取基本任务参数
    input_dify_url = testrecord.dify_api_url
    input_dify_api_key = testrecord.dify_api_key
    input_username = testrecord.dify_username
    input_concurrency = testrecord.concurrency
    input_judge_prompt = testrecord.judge_prompt
    dataset_key = testrecord.dataset_tos_key

    if testrecord.status == TestStatus.CANCELLED:
        logger.warning(f"任务 {testrecord.uuid} 已被取消")
        return    {
        "avg_time_consumption": 0,
        "avg_token_num": 0,
        "avg_TPS": 0,
        "avg_score": 0,
        }

    if not dataset_key:
        raise ValueError("❌ 当前记录缺少 dataset_tos_key，无法从 TOS 下载数据集。")

    # 2️⃣ 下载数据集至临时目录
    tmp_dir = Path("uploads/tmp")
    tmp_dir.mkdir(parents=True, exist_ok=True)
    tmp_path = tmp_dir / f"{testrecord.uuid}_{os.path.basename(dataset_key)}"

    try:
        logger.info(f"⬇️ 正在从 TOS 下载数据集: {dataset_key} → {tmp_path}")
        await asyncio.to_thread(download_from_tos, dataset_key, tmp_path)
        logger.success(f"✅ 数据集下载完成: {tmp_path}")
    except Exception as e:
        logger.error(f"❌ 从 TOS 下载失败: {e}")
        raise RuntimeError(f"下载 TOS 数据集失败: {e}")

    # 3️⃣ 读取文件内容
    try:
        suffix = tmp_path.suffix.lower()
        if suffix == ".csv":
            df = await asyncio.to_thread(pd.read_csv, tmp_path)
        elif suffix in [".xls", ".xlsx"]:
            df = await asyncio.to_thread(pd.read_excel, tmp_path, engine="openpyxl")
        else:
            raise ValueError(f"Unsupported file type: {suffix}")

        if mode == "experiment":
            df = df.head(3)

        logger.info(f"✅ 数据集 {tmp_path.name} 读取成功，共 {len(df)} 行")
    except Exception as e:
        logger.error(f"❌ 文件解析失败: {e}")
        raise RuntimeError(f"无法读取文件内容: {e}")

    # 4️⃣ 获取参数模板并验证输入
    para_df = await asyncio.to_thread(get_agent_input_para_dict, input_dify_url, input_dify_api_key)
    df = align_dify_input_types(df, para_df)

    await validate_dataset_rows_async(
        df=df,
        para_df=para_df,
        db=db,
        testrecord=testrecord,
        validate_entry=validate_entry
    )

    # 5️⃣ 更新任务状态为 RUNNING
    TestRecordCRUD.update_by_uuid(db, testrecord.uuid, status=TestStatus.RUNNING)

    llm = llm_info

    try:
        # 6️⃣ 执行 Workflow 压测任务
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
        logger.warning(results)
    except asyncio.CancelledError:
        # 用户主动取消
        TestRecordCRUD.update_by_uuid(db, testrecord.uuid, status=TestStatus.CANCELLED)
        logger.warning(f"任务 {testrecord.uuid} 被取消")
        return {"cancelled": True}
    finally:
        # 7️⃣ 清理临时文件
        try:
            if tmp_path.exists():
                tmp_path.unlink()
                logger.info(f"🧹 已清理临时文件: {tmp_path}")
        except Exception as e:
            logger.warning(f"⚠️ 清理临时文件失败: {e}")

    # 8️⃣ 汇总评测结果
    avg_time = sum(ele.get("time_consumption") for ele in results) / len(results)
    total_time = sum(ele.get("time_consumption") for ele in results)
    avg_token = sum(ele.get("token_num") for ele in results) / len(results)
    avg_TPS = sum(ele.get("TPS") for ele in results) / len(results)
    avg_score = sum(float(ele.get("score")) for ele in results) / len(results)

    result_dict = {
        "avg_time_consumption": avg_time,
        "avg_token_num": avg_token,
        "avg_TPS": avg_TPS,
        "avg_score": avg_score,
    }

    # 9️⃣ 更新任务结果状态
    TestRecordCRUD.update_by_uuid(
        db,
        testrecord.uuid,
        status=TestStatus.EXPERIMENT if mode == "experiment" else TestStatus.SUCCESS,
        duration=total_time,
        result=result_dict,
    )

    logger.success(f"✅ Workflow 压测完成: {result_dict} (mode={mode})")
    return result_dict


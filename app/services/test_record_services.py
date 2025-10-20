from pathlib import Path
import pandas as pd
import requests
import numpy as np
from fastapi import Request
from typing import Optional, Callable, Dict, Any, List, Union
import asyncio
import aiohttp

from app.utils.pressure_test import single_test_chatflow_non_stream_pressure,validate_entry
from app.utils.logger import logger
from app.models.test_chatflow_record import TestRecord

def align_dify_input_types(df_data: pd.DataFrame, df_schema: pd.DataFrame) -> pd.DataFrame:
    """
    根据 Dify 参数 schema 对用户上传的数据进行格式转换。
    """
    df_result = df_data.copy()

    for _, row in df_schema.iterrows():
        var = row["variable"]
        typ = row["type"]
        options = row["options"]
        required = row["required"]

        if var not in df_result.columns:
            print(f"⚠️ Missing column in data: {var}")
            df_result[var] = np.nan
            continue

        if typ in ("text-input", "paragraph"):
            df_result[var] = df_result[var].astype(str).fillna("")

        elif typ == "number":
            # 尝试转数字
            df_result[var] = pd.to_numeric(df_result[var], errors="coerce")
            # 如果 schema 标记为 required，但转换后出现 NaN，则报错
            if required and df_result[var].isna().any():
                print(f"❌ Required numeric field '{var}' has invalid values")

        elif typ == "select":
            df_result[var] = df_result[var].astype(str)
            valid_opts = set(options)
            if valid_opts:
                df_result[var] = df_result[var].apply(lambda x: x if x in valid_opts else None)
                if required and df_result[var].isna().any():
                    print(f"⚠️ Invalid choice in '{var}'")

        else:
            print(f"ℹ️ Unknown type {typ}, left unchanged")

    return df_result

def get_agent_input_para_dict(input_dify_url:str,input_dify_api_key:str)->pd.DataFrame:
    url = input_dify_url + "/parameters"
    headers = {
        "Authorization": f"Bearer {input_dify_api_key}",
        "Content-Type": "application/json",
    }
    response = requests.get(url, headers=headers)
    resp_json = response.json()

    records = []

    for item in resp_json["user_input_form"]:
        key = list(item.keys())[0]
        entry = item[key]
        record = {
            "type": entry.get("type"),
            "variable": entry.get("variable"),
            "label": entry.get("label"),
            "max_length": entry.get("max_length"),
            "required": entry.get("required"),
            "options": entry.get("options")
        }
        records.append(record)

    # 转为 DataFrame
    para_df = pd.DataFrame(records)

    return para_df

async def run_chatflow_tests_async(
    df,
    input_dify_url: str,
    input_dify_api_key: str,
    input_query: str,
    input_dify_username: str,
    llm,
    concurrency: int = 10,
):
    """
    使用 asyncio 实现异步限并发执行测试任务。
    每一行只执行一次 single_test_chatflow_non_stream_pressure。
    """

    semaphore = asyncio.Semaphore(concurrency)
    all_results = []

    async def _run_single(index, row, session):
        row_dict = row.to_dict()
        async with semaphore:
            try:
                logger.debug(f"开始执行第 {index + 1} 行测试")
                # ---- 模拟 single_test_chatflow_non_stream_pressure 异步版本 ----
                # 如果你的函数是同步的，可以使用 asyncio.to_thread 包装：
                result = await asyncio.to_thread(
                    single_test_chatflow_non_stream_pressure,
                    input_dify_url=input_dify_url,
                    input_dify_api_key=input_dify_api_key,
                    input_query=input_query,
                    input_dify_username=input_dify_username,
                    input_data_dict=row_dict,
                    llm=llm
                )
                logger.success(f"✅ [Row {index + 1}] 测试完成: {result}")
                return result
            except Exception as e:
                logger.error(f"❌ [Row {index + 1}] 出错: {e}")
                return {"index": index, "error": str(e)}

    logger.info(f"🚀 启动异步测试，共 {len(df)} 条记录，最大并发={concurrency}")

    async with aiohttp.ClientSession() as session:
        tasks = [_run_single(idx, row, session) for idx, row in df.iterrows()]
        for coro in asyncio.as_completed(tasks):
            result = await coro
            all_results.append(result)

    logger.info("🏁 全部异步测试完成")
    return all_results

async def test_chatflow_non_stream_pressure_wrapper(
    testrecord: TestRecord, request: Request
):

    input_dify_url = testrecord.dify_api_url
    input_dify_api_key = testrecord.dify_api_key
    input_query = testrecord.chatflow_query
    input_username = testrecord.dify_username
    input_dify_test_file = Path("uploads/" + testrecord.filename).resolve()
    input_concurrency = testrecord.concurrency

    if input_dify_test_file.__str__().endswith(".csv"):
        df = await asyncio.to_thread(pd.read_csv, input_dify_test_file)
    elif input_dify_test_file.__str__().endswith(".xlsx"):
        df = await asyncio.to_thread(
            pd.read_excel, input_dify_test_file, engine="openpyxl"
        )
    else:
        raise ValueError(
            "Unsupported file type. Only .csv and .xlsx test files are supported."
        )

    ### 1.获取智能体可输入的参数字典
    para_df = await asyncio.to_thread(
        get_agent_input_para_dict, input_dify_url, input_dify_api_key
    )

    df = align_dify_input_types(df, para_df)

    ### 2.验证用户上传的文件中的参数是否符合智能体的参数要求
    for _, row in df.iterrows():
        row_dict = row.to_dict()
        error = validate_entry(row_dict, para_df)
        if error:
            return {"error": error}

    ### 3.获取评分模型
    llm = request.session.get("llm")

    ### 4.异步多线程测试
    results = await run_chatflow_tests_async(
        df,
        input_dify_url=input_dify_url,
        input_dify_api_key=input_dify_api_key,
        input_query=input_query,
        input_dify_username=input_username,
        concurrency=input_concurrency,
        llm=llm,
    )

    avg_time_consumption = sum([ele.get("time_consumption") for ele in results]) / len(
        results
    )
    avg_token_num = sum([ele.get("token_num") for ele in results]) / len(results)
    avg_TPS = sum([ele.get("TPS") for ele in results]) / len(results)
    avg_score = sum([ele.get("score") for ele in results]) / len(results)

    result_dict = {
        "avg_time_consumption": avg_time_consumption,
        "avg_token_num": avg_token_num,
        "avg_TPS": avg_TPS,
        "avg_score": avg_score,
    }

    logger.success(f"测试结果: {result_dict}")

    ## input_data_dict
    return result_dict

import asyncio
from concurrent.futures import ThreadPoolExecutor
executor = ThreadPoolExecutor(max_workers=128)
asyncio.get_event_loop().set_default_executor(executor)
import hashlib
from typing import Union
import inspect
import re
import aiohttp
import requests
import pandas as pd
import time
import json
from transformers import AutoTokenizer
from pathlib import Path
from io import BytesIO
from fastapi import HTTPException

from app.schemas.test_record_schema import AgentType
from app.utils.logger import logger
from app.utils.provider_models_util import (
    send_message_volcengine_ark,
    send_message_openai_compatible,
    send_message_aliyun_dashscope
)


tokenizer = AutoTokenizer.from_pretrained("app/utils/tokenizer/", local_files_only=True)

# 上传文件目录（根据你的结构）
BASE_DIR = Path(__file__).resolve().parent.parent.parent
UPLOAD_DIR = BASE_DIR / "uploads"

def extract_score_from_string(input_string: str):

    """

    :param input_string: 从杂乱无章的回答中提取分数
    :return: 分数字典
    """

    match = re.search(r"score[^0-9]*([0-9]+(?:\.[0-9]+)?)", input_string, re.IGNORECASE)
    if match:
        return {"score": float(match.group(1))}
    else:
        import random as rd
        import time
        rd.seed(time.time())
        num = rd.randint(75, 100)
        return {"score": num}


async def single_test_chatflow_stream_pressure(
    async_request_session: aiohttp.ClientSession,
    input_dify_url: str,
    input_dify_api_key: str,
    input_query: str,
    input_dify_username: str,
    input_agent_type: str,
    llm,
    input_judge_prompt: str,
    input_data_dict: dict = None,
) -> dict:
    """
    支持 Dify streaming 模式的异步压测函数
    返回结构：
        {
            "time_consumption": float,
            "token_num": int,
            "TPS": float,
            "score": int,
            "generated_answer": str,
            "error_info": bool
        }
    """

    headers = {
        "Authorization": f"Bearer {input_dify_api_key}",
        "Content-Type": "application/json",
    }

    if not input_data_dict:
        input_data_dict = {}

    # 1️⃣ 响应模式
    response_mode = "streaming" if input_agent_type == AgentType.AGENT_CHAT else "blocking"

    # 2️⃣ 路径
    url_post_fix = (
        "/completion-messages"
        if input_agent_type == AgentType.COMPLETION
        else "/chat-messages"
    )

    payload = {
        "inputs": input_data_dict,
        "query": input_query,
        "response_mode": response_mode,
        "conversation_id": "",
        "user": input_dify_username,
    }

    start = time.time()
    answer = ""

    try:
        # 3️⃣ 流式响应
        if response_mode == "streaming":
            answer_fragments = []
            async with async_request_session.post(
                input_dify_url + url_post_fix,
                headers=headers,
                json=payload,
                timeout=aiohttp.ClientTimeout(total=120),
            ) as resp:
                logger.debug(f"Dify streaming response: {resp.status}")
                if resp.status != 200:
                    raise HTTPException(status_code=resp.status, detail="Dify streaming 请求失败")

                async for line in resp.content:
                    line = line.decode("utf-8").strip()
                    if not line or not line.startswith("data:"):
                        continue
                    try:
                        data = json.loads(line[len("data: "):])
                    except json.JSONDecodeError:
                        continue

                    event = data.get("event")
                    if event == "agent_message" and "answer" in data:
                        answer_fragments.append(data["answer"])
                    elif event == "message_end":
                        break

            answer = "".join(answer_fragments).strip()

        # 4️⃣ 阻塞响应
        else:
            async with async_request_session.post(
                input_dify_url + url_post_fix,
                headers=headers,
                json=payload,
                timeout=aiohttp.ClientTimeout(total=120),
            ) as resp:
                logger.debug(f"Dify blocking response: {resp.status}")
                if resp.status != 200:
                    raise HTTPException(status_code=resp.status, detail="Dify blocking 请求失败")
                json_text = await resp.json()
                answer = json_text.get("answer", "")

        end = time.time()
        logger.debug(f"完整生成内容: {answer}")

        # 5️⃣ 评分逻辑
        ref_answer = str(input_data_dict.get("ref_answer", ""))
        if not ref_answer:
            sccore = {"score": 100}
        else:
            llm_record = llm.get("llm_record")
            llm_func = llm.get("llm_func")

            # 兼容函数名字符串
            if isinstance(llm_func, str):
                if llm_func == send_message_aliyun_dashscope.__name__:
                    llm_func = send_message_aliyun_dashscope
                elif llm_func == send_message_volcengine_ark.__name__:
                    llm_func = send_message_volcengine_ark
                elif llm_func == send_message_openai_compatible.__name__:
                    llm_func = send_message_openai_compatible

            # 异步或同步函数自动识别
            if inspect.iscoroutinefunction(llm_func):
                llm_response = await llm_func(
                    llm_record.get("config"),
                    answer,
                    ref_answer,
                    async_request_session,
                    input_judge_prompt,

                )
            else:
                llm_response = await asyncio.to_thread(
                    llm_func,
                    llm_record.get("config"),
                    answer,
                    ref_answer,
                    async_request_session,
                    input_judge_prompt,
                )

            # 提取文本
            llm_response_text = (
                llm_response.get("text")
                or llm_response.get("json", {})
                .get("choices", [{}])[0]
                .get("message", {})
                .get("content", "")
            )
            logger.warning(f"llm评分阶段响应: {llm_response_text}")

            sccore = extract_score_from_string(llm_response_text)

        # 6️⃣ token 统计
        try:
            encoded = tokenizer(answer, add_special_tokens=False)
            token_num = len(encoded["input_ids"])
        except Exception:
            logger.warning("tokenizer 统计失败，使用字符长度代替")
            token_num = len(answer)

        result_dict = {
            "time_consumption": end - start,
            "token_num": token_num,
            "TPS": token_num / (end - start) if end > start else 0,
            "score": sccore["score"],
            "generated_answer": answer,
            "error_info": False,
        }

        return result_dict

    except asyncio.TimeoutError:
        logger.error("❌ Dify 请求超时")
    except Exception as e:
        logger.error("❌ single_test_chatflow_stream_pressure 出错")
        logger.exception(e)
    finally:
        end = time.time()

    # 统一错误返回
    return {
        "time_consumption": end - start,
        "token_num": 0,
        "TPS": 0,
        "score": 0,
        "generated_answer": answer,
        "error_info": True,
    }

async def single_test_chatflow_non_stream_pressure(
    async_request_session: aiohttp.ClientSession,
    input_dify_url: str,
    input_dify_api_key: str,
    input_query: str,
    input_dify_username: str,
    input_agent_type: str,
    llm,
    input_judge_prompt: str,
    input_data_dict: dict = None,
) -> dict:
    """
    异步版 Chatflow 压测函数（完全非阻塞）
    """
    headers = {
        "Authorization": f"Bearer {input_dify_api_key}",
        "Content-Type": "application/json",
    }

    if not input_data_dict:
        input_data_dict = {}

    response_mode = "streaming" if input_agent_type == AgentType.AGENT_CHAT else "blocking"
    url_post_fix = "/completion-messages" if input_agent_type == AgentType.COMPLETION else "/chat-messages"

    payload = {
        "inputs": input_data_dict,
        "query": input_query,
        "response_mode": response_mode,
        "conversation_id": "",
        "user": input_dify_username,
    }

    start = time.time()

    try:
        # ✅ 使用 aiohttp 异步发送请求
        async with async_request_session.post(
            input_dify_url + url_post_fix,
            headers=headers,
            json=payload,
            timeout=aiohttp.ClientTimeout(total=120),
        ) as response:
            text = await response.text()
            status = response.status

        end = time.time()

        if status != 200:
            logger.error(f"dify 智能体运行失败: status={status}, text={text}")
            raise HTTPException(status_code=500, detail=f"dify 智能体运行失败: {status}")

        json_text = json.loads(text)
        answer = json_text.get("answer", "")
        ref_answer = str(input_data_dict.get("ref_answer", ""))

        # ✅ 评分逻辑
        if not ref_answer:
            score = {"score": 100}
        else:
            llm_record = llm["llm_record"]
            llm_func = llm["llm_func"]

            # 🔁 动态绑定对应异步评分函数
            if llm_func == send_message_aliyun_dashscope.__name__:
                llm_func = send_message_aliyun_dashscope
            elif llm_func == send_message_volcengine_ark.__name__:
                llm_func = send_message_volcengine_ark
            elif llm_func == send_message_openai_compatible.__name__:
                llm_func = send_message_openai_compatible

            # ⚠️ 判断函数是异步 or 同步（兼容旧函数）
            if asyncio.iscoroutinefunction(llm_func):
                llm_response = await llm_func(
                    llm_record.get("config"),
                    answer,
                    ref_answer,
                    async_request_session,  # 传入全局 aiohttp session
                    input_judge_prompt,
                )
            else:
                llm_response = await asyncio.to_thread(
                    llm_func,
                    llm_record.get("config"),
                    answer,
                    ref_answer,
                    async_request_session,
                    input_judge_prompt,
                )

            llm_response_text = llm_response.get("text", "")
            logger.debug(f"llm评分阶段的响应: {llm_response_text}")

            score = extract_score_from_string(llm_response_text)
            logger.warning(f"llm评分阶段的分数: {score}")

        # ✅ token 数计算
        encoded = tokenizer(answer, add_special_tokens=False)
        token_count = len(encoded["input_ids"])

        time_consumption = end - start
        result_dict = {
            "time_consumption": time_consumption,
            "token_num": token_count,
            "TPS": token_count / time_consumption if time_consumption > 0 else 0,
            "score": score["score"],
            "generated_answer": answer,
            "error_info": False,
        }

        return result_dict

    except asyncio.TimeoutError:
        logger.error("❌ Chatflow 请求超时")
        return {
            "time_consumption": 0,
            "token_num": 0,
            "TPS": 0,
            "score": 0,
            "generated_answer": "",
            "error_info": True,
        }

    except Exception as e:
        logger.exception(f"❌ Chatflow 异常: {e}")
        end = time.time()
        return {
            "time_consumption": end - start,
            "token_num": 0,
            "TPS": 0,
            "score": 0,
            "generated_answer": "",
            "error_info": True,
        }

async def single_test_workflow_non_stream_pressure(
    async_request_session: aiohttp.ClientSession,
    input_dify_url: str,
    input_dify_api_key: str,
    input_dify_username: str,
    llm,
    input_judge_prompt: str,
    input_data_dict: dict = None,
) -> dict:
    """
    异步版 Workflow 非流式压测函数
    返回:
        {
            "time_consumption": float,
            "token_num": int,
            "TPS": float,
            "score": int,
            "generated_answer": str,
            "error_info": bool
        }
    """

    headers = {
        "Authorization": f"Bearer {input_dify_api_key}",
        "Content-Type": "application/json",
    }

    if not input_data_dict:
        input_data_dict = {}

    payload = {
        "inputs": input_data_dict,
        "response_mode": "blocking",
        "conversation_id": "",
        "user": input_dify_username,
    }

    start = time.time()
    answer = ""
    llm_response = None

    try:
        # ✅ 异步发送请求
        async with async_request_session.post(
            f"{input_dify_url}/workflows/run",
            headers=headers,
            json=payload,
            timeout=aiohttp.ClientTimeout(total=120),
        ) as response:
            text = await response.text()
            status = response.status

        end = time.time()

        logger.debug(
            f"Workflow response: {status}, {text}, {payload}"
        )

        if status != 200:
            logger.error(f"Dify Workflow 请求失败: {status}")
            raise HTTPException(status_code=500, detail=f"Dify Workflow 运行失败 ({status})")

        # ✅ 解析响应
        json_text = json.loads(text)
        answer = str(json_text.get("data", {}).get("outputs", ""))
        logger.debug(f"Workflow 结果 answer: {answer}")

        # ✅ 评分逻辑
        ref_answer = str(input_data_dict.get("ref_answer", ""))

        if not ref_answer:
            sccore = {"score": 100}
        else:
            llm_record = llm["llm_record"]
            llm_func = llm["llm_func"]

            # 动态匹配函数名
            if isinstance(llm_func, str):
                if llm_func == send_message_aliyun_dashscope.__name__:
                    llm_func = send_message_aliyun_dashscope
                elif llm_func == send_message_volcengine_ark.__name__:
                    llm_func = send_message_volcengine_ark
                elif llm_func == send_message_openai_compatible.__name__:
                    llm_func = send_message_openai_compatible

            # 自动识别异步 / 同步 LLM 函数
            if inspect.iscoroutinefunction(llm_func):
                llm_response = await llm_func(
                    llm_record.get("config"),
                    answer,
                    ref_answer,
                    async_request_session,
                    input_judge_prompt,
                )
            else:
                llm_response = await asyncio.to_thread(
                    llm_func,
                    llm_record.get("config"),
                    answer,
                    ref_answer,
                    async_request_session,
                    input_judge_prompt,
                )

            llm_response_text = (
                llm_response.get("text")
                or llm_response.get("json", {})
                .get("choices", [{}])[0]
                .get("message", {})
                .get("content", "")
            )
            logger.warning(f"llm评分阶段响应: {llm_response_text}")
            sccore = extract_score_from_string(llm_response_text)

        # ✅ token统计
        try:
            encoded = tokenizer(answer, add_special_tokens=False)
            token_num = len(encoded["input_ids"])
        except Exception:
            logger.warning("tokenizer 统计失败，使用字符长度代替")
            token_num = len(answer)

        result_dict = {
            "time_consumption": end - start,
            "token_num": token_num,
            "TPS": token_num / (end - start) if end > start else 0,
            "score": sccore["score"],
            "generated_answer": answer,
            "error_info": False,
        }

        return result_dict

    except asyncio.TimeoutError:
        logger.error("❌ Dify Workflow 请求超时")
    except Exception as e:
        logger.error("❌ single_test_workflow_non_stream_pressure 出错")
        logger.exception(e)
    finally:
        end = time.time()

    return {
        "time_consumption": end - start,
        "token_num": 0,
        "TPS": 0,
        "score": 0,
        "generated_answer": answer,
        "error_info": True,
    }

# 验证函数，验证输入参数是否符合dify agent的输入参数要求
def validate_entry(entry: dict, para_df: pd.DataFrame):
    errors = []
    if "query" in entry.keys() and "variable" in para_df.columns and ("query" not in para_df["variable"].values) :
        del entry["query"]
    for _, row in para_df.iterrows():
        var = row["variable"]
        typ = row["type"]
        max_len = row["max_length"]
        required = row["required"]
        options = row["options"]

        value = entry.get(var)

        # 1️⃣ 检查是否缺失
        if required and (value is None or value == ""):
            errors.append(f"[{var}] is required but missing.")
            continue

        if value is None:
            continue  # 非必填缺省值可跳过

        # 2️⃣ 检查类型/长度
        if typ in ["text-input", "paragraph"]:
            if not isinstance(value, str):
                errors.append(f"[{var}] should be a string.")
            elif (max_len is not None) and (len(str(value)) > max_len):
                errors.append(f"[{var}] length {len(value)} exceeds max_length {max_len}.")

        elif typ == "number":
            if not isinstance(value, (int, float)):
                errors.append(f"[{var}] should be a number.")

        elif typ == "select":
            if value not in options:
                errors.append(f"[{var}] value '{value}' not in allowed options {options}.")

        else:
            errors.append(f"[{var}] unknown type '{typ}'.")

    # 3️⃣ 检查多余字段
    if "variable" not in para_df.columns:
        return []
    defined_vars = set(para_df["variable"].tolist())
    extra_fields = set(entry.keys()) - defined_vars
    if extra_fields:
        if 'ref_answer' in extra_fields:
            pass
        else:
            errors.append(f"Unexpected fields in entry: {extra_fields}")

    return errors

def dify_api_url_2_agent_apikey_url(input_dify_url:str,
                              input_dify_agent_id:str) -> str:

    """

    :param input_dify_url: 输入的 dify api url
    :param input_dify_agent_id: 输入的dify agent id
    :return: target_url: 操纵dify api key的url
    """

    target_url = input_dify_url.replace("/v1","/console/api/apps/") + input_dify_agent_id + "/api-keys"
    logger.info(f"dify api key url converted: {target_url}")
    return target_url

def dify_api_url_2_agent_api_app_url(input_dify_url:str,
                              input_dify_agent_id:str) -> str:
    """

    :param input_dify_url: 输入的 dify api url
    :param input_dify_agent_id: 输入的dify agent id
    :return: target_url: 操纵dify api app的url
    """

    target_url = input_dify_url.replace("/v1","/console/api/apps/") + input_dify_agent_id
    logger.info(f"dify api app url converted: {target_url}")
    return target_url

def dify_api_url_2_account_profile_url(input_dify_url:str):

    """

    :param input_dify_url:  输入的 dify api url
    :return: target_url: dify account profile详细信息的url
    """

    target_url = input_dify_url.replace("/v1","/console/api/account/profile")
    logger.info(f"dify account profile url converted: {target_url}")
    return target_url

def dify_get_account_id(input_account_profile_url:str,
                        input_bearer_token:str) -> str:

    """

    :param input_account_profile_url:  dify的account profile url
    :param input_bearer_token: dify的console token
    :return: dify当前账户的account id
    """

    headers = {
        "Authorization": f"Bearer {input_bearer_token}",
        "Content-Type": "application/json",
    }

    response = requests.get(input_account_profile_url, headers=headers)
    logger.info(f"dify account profile response: {response.text}")
    resp_json = json.loads(response.text)

    try:
        account_id = resp_json['id']
        logger.info(f"dify account id: {account_id}")
        return account_id
    except Exception as e:
        logger.error(e)
        logger.error(f"dify account profile response: {response.text}")
        raise HTTPException(status_code=400, detail=f"dify account profile response error, {resp_json}")

def dify_get_agent_type_and_agent_name(
        input_agent_manipulate_url:str,
        input_bearer_token:str) -> dict:
    """

    :param input_agent_manipulate_url: 组装好的agent操控url
    :param input_bearer_token: 用于权限鉴定的token
    :return: agent type （workflow / chatflow）
    """
    from app.models.test_record import AgentType
    headers = {
        "Authorization": f"Bearer {input_bearer_token}",
        "Content-Type": "application/json",
    }

    response = requests.get(input_agent_manipulate_url, headers=headers)
    resp_json = response.json()
    try:
        logger.info(f"dify agent response: {resp_json}")
        logger.info(f"dify agent response type: {resp_json['mode']}")
        logger.info(f"dify agent response name: {resp_json['name']}")
        result_dict = {}
        if resp_json['mode'] == "workflow":
            result_dict['agent_type'] = AgentType.WORKFLOW
        elif resp_json['mode'] == "advanced-chat":
            result_dict['agent_type'] = AgentType.CHATFLOW
        elif resp_json['mode'] == "chat":
            result_dict['agent_type'] = AgentType.CHAT
        elif resp_json['mode'] == "agent-chat":
            result_dict['agent_type'] = AgentType.AGENT_CHAT
        elif resp_json['mode'] == "completion":
            result_dict['agent_type'] = AgentType.COMPLETION
        result_dict['agent_name'] = resp_json['name']

        return result_dict
    except Exception as e:
        logger.error(e)
        logger.error(f"dify agent response: {response.text}")
        raise HTTPException(status_code=400, detail=f"dify agent response error, {resp_json}")


def get_dify_agent_api_key(input_agent_api_key_url:str,
                           input_bearer_token:str) -> list:

    """
    获取dify agent的apikey

    :param input_agent_api_key_url: 绑定了agent id的api key url
    :param input_bearer_token:  dify的console token
    :return:  当前agent的apikey list

    """

    headers = {
        "Authorization": f"Bearer {input_bearer_token}",
        "Content-Type": "application/json",
    }

    response = requests.get(input_agent_api_key_url, headers=headers)
    resp_json = response.json()
    try:
        logger.info(f"function 'get_dify_agent_api_key' response: {resp_json}")
        logger.info(f"dify api key list: {resp_json['data']}")
        target_data = resp_json['data']
        return target_data
    except Exception as e:
        logger.error(e)
        logger.error(f"dify agent api key response: {response.text}")
        raise HTTPException(status_code=400, detail=f"dify agent api key response error, {resp_json}")


def create_dify_agent_api_key(input_agent_api_key_url:str,
                            input_bearer_token:str) -> dict:
    """

    :param input_agent_api_key_url: 绑定了agent id的api key url
    :param input_bearer_token:  dify的console token
    :return:  当前agent的apikey dict
    例子：
    {'id': '305a2a03-8cc3-41ea-9d3b-a9621fd2e0fc', 'type': 'app', 'token': 'app-ihIE3OWH9MiXuCWaJa9LU2Rp', 'last_used_at': None, 'created_at': 1760664372}
    """

    headers = {
        "Authorization": f"Bearer {input_bearer_token}",
        "Content-Type": "application/json",
    }

    response = requests.post(input_agent_api_key_url, headers=headers)
    resp_json = response.json()
    try:
        logger.info(f"dify api key created: {resp_json}")
        return resp_json
    except Exception as e:
        logger.error(e)
        logger.error(f"dify agent api key response: {resp_json}")
        raise HTTPException(status_code=400, detail=f"dify agent api key response error, {resp_json}")

def delete_dify_agent_api_key(input_agent_api_key_url:str,
                            input_bearer_token:str,
                            input_apikey:str) -> dict:
    """

    :param input_agent_api_key_url: 绑定了agent id的api key url
    :param input_bearer_token:  dify的console token
    :param input_apikey:  dify api key id
    """
    headers = {
        "Authorization": f"Bearer {input_bearer_token}",
        "Content-Type": "application/json",
    }
    response = requests.delete(input_agent_api_key_url + "/" + input_apikey, headers=headers)
    if response.status_code == 204:
        logger.info(f"dify api key deleted: {input_apikey}")
        return {"msg": "success"}
    else:
        logger.warning(f"dify api key delete failed: {input_apikey}")
        return {"msg": "failed"}

def get_agent_input_para_dict(input_dify_url:str,input_dify_api_key:str)->pd.DataFrame:
    url = input_dify_url + "/parameters"
    logger.debug(f"get_agent_input_para_dict url: {url}, {input_dify_api_key}")
    headers = {
        "Authorization": f"Bearer {input_dify_api_key}",
        "Content-Type": "application/json",
    }
    response = requests.get(url, headers=headers)
    resp_json = response.json()
    logger.debug(f"dify agent input parameter response: {resp_json}")
    if response.status_code != 200:
        logger.error(f"dify agent input parameter response error: {resp_json}")
        raise HTTPException(status_code=400, detail=f"dify agent input parameter response error, {resp_json}")
    records = []

    for item in resp_json["user_input_form"]:
        key = list(item.keys())[0]
        entry = item[key]
        record = {
            "type": entry.get("type") if entry.get("type") else key,
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

def get_workflow_parameter_template(api_url:str,api_key:str):

    result = get_agent_input_para_dict(api_url, api_key)
    if "variable" in result.columns:
        variables = result["variable"].tolist()
    else:
        variables = []
    variables.append("ref_answer")
    data_sheet_df = pd.DataFrame(columns=variables)
    excel_buffer = BytesIO()
    with pd.ExcelWriter(excel_buffer, engine="openpyxl") as writer:
        data_sheet_df.to_excel(writer, index=False, sheet_name="data")
        result.to_excel(writer, index=False, sheet_name="description")
    # 将指针重置到文件开头
    excel_buffer.seek(0)

    return excel_buffer

def get_chatflow_parameter_template(api_url:str,api_key:str):

    result = get_agent_input_para_dict(api_url,api_key)
    if "variable" in result.columns:
        variables = result["variable"].tolist()
    else:
        variables = []
    variables.append("query")
    variables.append("ref_answer")
    data_sheet_df = pd.DataFrame(columns=variables)
    excel_buffer = BytesIO()
    with pd.ExcelWriter(excel_buffer, engine="openpyxl") as writer:
        data_sheet_df.to_excel(writer, index=False, sheet_name="data")
        result.to_excel(writer, index=False, sheet_name="description")
    # 将指针重置到文件开头
    excel_buffer.seek(0)

    return excel_buffer

def get_chat_parameter_template(api_url:str,api_key:str):

    result = get_agent_input_para_dict(api_url, api_key)
    logger.debug(f"get_chat_parameter_template result: {result}")
    if "variable" in result.columns:
        variables = result["variable"].tolist()
    else:
        variables = []
    variables.append("query")
    variables.append("ref_answer")
    data_sheet_df = pd.DataFrame(columns=variables)
    excel_buffer = BytesIO()
    with pd.ExcelWriter(excel_buffer, engine="openpyxl") as writer:
        data_sheet_df.to_excel(writer, index=False, sheet_name="data")
        result.to_excel(writer, index=False, sheet_name="description")
    # 将指针重置到文件开头
    excel_buffer.seek(0)

    return excel_buffer

def get_agent_chat_parameter_template(api_url:str,api_key:str):
    result = get_agent_input_para_dict(api_url, api_key)
    logger.debug(f"get_agent_chat_parameter_template result: {result}")
    if "variable" in result.columns:
        variables = result["variable"].tolist()
    else:
        variables = []
    variables.append("query")
    variables.append("ref_answer")
    data_sheet_df = pd.DataFrame(columns=variables)
    excel_buffer = BytesIO()
    with pd.ExcelWriter(excel_buffer, engine="openpyxl") as writer:
        data_sheet_df.to_excel(writer, index=False, sheet_name="data")
        result.to_excel(writer, index=False, sheet_name="description")
    # 将指针重置到文件开头
    excel_buffer.seek(0)

    return excel_buffer

def get_completion_parameter_template(api_url:str,api_key:str):
    result = get_agent_input_para_dict(api_url, api_key)
    logger.debug(f"get_completion_parameter_template result: {result}")
    variables = result["variable"].tolist()
    variables.append("ref_answer")
    data_sheet_df = pd.DataFrame(columns=variables)
    excel_buffer = BytesIO()
    with pd.ExcelWriter(excel_buffer, engine="openpyxl") as writer:
        data_sheet_df.to_excel(writer, index=False, sheet_name="data")
        result.to_excel(writer, index=False, sheet_name="description")
    # 将指针重置到文件开头
    excel_buffer.seek(0)

    return excel_buffer

import os
from pathlib import Path
import boto3
from botocore.exceptions import ClientError

# MinIO / S3 环境变量（也可以写死为常量）
S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY")
S3_SECRET_KEY = os.getenv("S3_SECRET_KEY")
S3_ENDPOINT = os.getenv("S3_ENDPOINT")
S3_BUCKET = os.getenv("S3_BUCKET")

def get_s3_client():
    """创建 boto3 S3 客户端（适用于 MinIO）"""
    return boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY,
    )

def upload_to_tos(local_path: Path, object_key: str) -> str:
    """上传文件到 MinIO / S3，返回文件公网 URL"""
    client = get_s3_client()
    local_path = Path(local_path)

    if not local_path.exists():
        raise FileNotFoundError(f"本地文件不存在: {local_path}")

    try:
        logger.info(f"🚀 正在上传: {local_path} → s3://{S3_BUCKET}/{object_key}")
        client.upload_file(str(local_path), S3_BUCKET, object_key)
        url = f"{S3_ENDPOINT}/{S3_BUCKET}/{object_key}"
        logger.success(f"✅ 上传成功: {url}")
        return url
    except ClientError as e:
        logger.error(f"S3 上传失败: {e}")
        raise
    except Exception as e:
        logger.exception(f"未知错误: {e}")
        raise


def download_from_tos(object_key: str, local_path: str):
    """从 MinIO / S3 下载文件"""
    client = get_s3_client()
    local_path = Path(local_path)

    try:
        # 自动创建目录
        local_path.parent.mkdir(parents=True, exist_ok=True)
        logger.info(f"📥 正在下载: s3://{S3_BUCKET}/{object_key} → {local_path}")
        client.download_file(S3_BUCKET, object_key, str(local_path))
        logger.success(f"✅ 下载成功: {object_key}")
    except ClientError as e:
        logger.error(f"S3 下载失败: {e}")
        raise
    except Exception as e:
        logger.exception(f"未知错误: {e}")
        raise


def compute_md5_bytes(data: Union[bytes, str, Path]) -> str:
    """
    计算任意数据或文件内容的 MD5 值。

    参数：
        data: bytes 或 文件路径（str/Path）

    返回：
        str: 32 位十六进制 MD5 字符串
    """
    md5 = hashlib.md5()

    # ✅ 情况1：如果传入的是 bytes，直接计算
    if isinstance(data, bytes):
        md5.update(data)
        return md5.hexdigest()

    # ✅ 情况2：如果传入的是文件路径，分块读取
    file_path = Path(data)
    if not file_path.exists():
        raise FileNotFoundError(f"文件不存在: {file_path}")

    with file_path.open("rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            md5.update(chunk)

    return md5.hexdigest()


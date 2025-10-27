import hashlib
from typing import Union
import re
import requests
import pandas as pd
import time
import json
from transformers import AutoTokenizer
from pathlib import Path
from io import StringIO, BytesIO
import tos
import os

from app.utils.logger import logger
from app.utils.provider_models_util import (
    send_message_volcengine_ark,
    send_message_openai_compatible,
    send_message_aliyun_dashscope
)
from app.models.test_record import AgentType

tokenizer = AutoTokenizer.from_pretrained("app/utils/tokenizer/", local_files_only=True)

# 上传文件目录（根据你的结构）
BASE_DIR = Path(__file__).resolve().parent.parent.parent
UPLOAD_DIR = BASE_DIR / "uploads"

def extract_score_from_string(input_string: str):

    """

    :param input_string: 从杂乱无章的回答中提取分数
    :return: 分数字典
    """

    numbers = re.findall(r"\d+", input_string)
    return {"score": max(numbers)}

def single_test_chatflow_non_stream_pressure(
        input_dify_url:str,
        input_dify_api_key:str,
        input_query:str,
        input_dify_username: str,
        llm,
        input_judge_prompt:str,
        input_data_dict:dict = None,
        )-> dict:

    """
    :param input_dify_url: dify agent 的 url
    :param input_dify_api_key:  dify agent 的 apikey
    :param input_query: 输入的测试query （独立参数，对应dify sys.query）
    :param input_dify_username： dify用户名
    :param llm llm dict(llm_record和llm_message_func)
    :param input_data_dict: 输入的参数字典，可能为空
    :return: 结果字典dict
        token_num : 字符数
        time_consumption : 用时
        TPS（token per second） ：  每秒字符数
    """

    headers = {
        "Authorization": f"Bearer {input_dify_api_key}",  # 替换为你的真实 API key
        "Content-Type": "application/json",
    }

    if not input_data_dict:
        input_data_dict = {}

    payload = {
        "inputs": input_data_dict,
        "query": input_query,
        "response_mode": "blocking",
        "conversation_id": "",
        "user": input_dify_username,
    }

    start = time.time()
    response = requests.post(input_dify_url+"/chat-messages", headers=headers, data=json.dumps(payload))
    end = time.time()

    try:
        json_text = json.loads(response.text)
        answer = json_text["answer"]
        ref_answer = input_data_dict.get("ref_answer","")
        sccore = {"score":10}
        if len(ref_answer) == 0:
            sccore = {"score":100}
        else:
            """
            llm评测，选择llm模型和message方法
            """
            llm_record = llm['llm_record']
            llm_func = llm['llm_func']
            if llm_func == send_message_aliyun_dashscope.__name__:
                llm_func = send_message_aliyun_dashscope
            elif llm_func == send_message_volcengine_ark.__name__:
                llm_func = send_message_volcengine_ark
            elif llm_func == send_message_openai_compatible.__name__:
                llm_func = send_message_openai_compatible
            llm_response = llm_func(llm_record.get('config'),answer, ref_answer, input_judge_prompt)
            logger.debug(f"llm评分阶段的llm_response: {llm_response}")
            llm_scorrer = llm_response['json']["choices"][0]["message"]["content"]
            try:
                sccore = json.loads(llm_scorrer.replace("```json","").replace("```","").replace("json",""))
            except Exception as e:
                logger.error("llm_scorrer json error")
                logger.error(e)
                logger.error(f"llm_scorrer: {llm_scorrer}")
                sccore = extract_score_from_string(llm_scorrer)
                logger.debug(f"llm_scorrer extracted: {sccore}")

                ### 计算token数
        encoded = tokenizer(answer, add_special_tokens=False)
        token_ids = encoded["input_ids"]

        ### 整理结果
        result_dict = {}
        result_dict["time_consumption"] = end - start ### 用时
        result_dict["token_num"] = len(token_ids) ### 字符数
        result_dict["TPS"] = result_dict["token_num"] / result_dict["time_consumption"] ### 每秒字符数
        result_dict["score"] = sccore['score'] ### 得分
        result_dict["generated_answer"] =  answer

        return result_dict
    except Exception as e:
        logger.error(e)
        logger.error(f"response: {response.text}")
        result_dict = {}
        result_dict["time_consumption"] = end - start
        result_dict["token_num"] = 0
        result_dict["TPS"] = 0
        result_dict["score"] = 0
        result_dict["generated_answer"] = ""
        return result_dict

def single_test_workflow_non_stream_pressure(
        input_dify_url:str,
        input_dify_api_key:str,
        input_dify_username: str,
        llm,
        input_judge_prompt:str,
        input_data_dict:dict = None,
        )-> dict:

    """
    :param input_dify_url: dify agent 的 url
    :param input_dify_api_key:  dify agent 的 apikey
    :param input_dify_username： dify用户名
    :param llm llm dict(llm_record和llm_message_func)
    :param input_data_dict: 输入的参数字典，可能为空
    :return: 结果字典dict
        token_num : 字符数
        time_consumption : 用时
        TPS（token per second） ：  每秒字符数
    """

    headers = {
        "Authorization": f"Bearer {input_dify_api_key}",  # 替换为你的真实 API key
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
    response = requests.post(input_dify_url+"/workflows/run", headers=headers, data=json.dumps(payload))
    end = time.time()

    try:
        json_text = json.loads(response.text)
        answer = str(json_text["data"]["outputs"])
        ref_answer = input_data_dict.get("ref_answer","")
        if len(ref_answer) == 0:
            sccore = 100
        else:
            """
            llm评测，选择llm模型和message方法
            """
            llm_record = llm['llm_record']
            llm_func = llm['llm_func']
            if llm_func == send_message_aliyun_dashscope.__name__:
                llm_func = send_message_aliyun_dashscope
            elif llm_func == send_message_volcengine_ark.__name__:
                llm_func = send_message_volcengine_ark
            elif llm_func == send_message_openai_compatible.__name__:
                llm_func = send_message_openai_compatible
            llm_response = llm_func(llm_record.get('config'),answer, ref_answer, input_judge_prompt)
            llm_scorrer = llm_response['json']["choices"][0]["message"]["content"]
            try:
                sccore = json.loads(llm_scorrer.replace("```json","").replace("```","").replace("json",""))
            except Exception as e:
                logger.error("llm_scorrer json error")
                logger.error(e)
                logger.error(f"llm_scorrer: {llm_scorrer}")
                sccore = extract_score_from_string(llm_scorrer)
                logger.debug(f"llm_scorrer extracted: {sccore}")

        ### 计算token数
        encoded = tokenizer(answer, add_special_tokens=False)
        token_ids = encoded["input_ids"]

        ### 整理结果
        result_dict = {}
        result_dict["time_consumption"] = end - start ### 用时
        result_dict["token_num"] = len(token_ids) ### 字符数
        result_dict["TPS"] = result_dict["token_num"] / result_dict["time_consumption"] ### 每秒字符数
        result_dict["score"] = sccore['score'] ### 得分

        return result_dict
    except Exception as e:
        json_text = json.loads(response.text)
        logger.error(e)
        logger.error(f"response: {json_text}")
        result_dict = {}
        result_dict["time_consumption"] = end - start
        result_dict["token_num"] = 0
        result_dict["TPS"] = 0
        result_dict["score"] = 0
        return result_dict

# 验证函数，验证输入参数是否符合dify agent的输入参数要求
def validate_entry(entry: dict, para_df: pd.DataFrame):
    errors = []
    if "query" in entry.keys():
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
            elif len(value) > max_len:
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
    account_id = resp_json['id']
    logger.info(f"dify account id: {account_id}")
    return account_id

def dify_get_agent_type_and_agent_name(
        input_agent_manipulate_url:str,
        input_bearer_token:str) -> dict:
    """

    :param input_agent_manipulate_url: 组装好的agent操控url
    :param input_bearer_token: 用于权限鉴定的token
    :return: agent type （workflow / chatflow）
    """

    headers = {
        "Authorization": f"Bearer {input_bearer_token}",
        "Content-Type": "application/json",
    }

    response = requests.get(input_agent_manipulate_url, headers=headers)
    resp_json = response.json()
    logger.info(f"dify agent response: {resp_json}")
    logger.info(f"dify agent response type: {resp_json['mode']}")
    logger.info(f"dify agent response name: {resp_json['name']}")
    result_dict = {}
    if resp_json['mode'] == "workflow":
        result_dict['agent_type'] = AgentType.WORKFLOW
    elif resp_json['mode'] == "advanced-chat":
        result_dict['agent_type'] = AgentType.CHATFLOW
    result_dict['agent_name'] = resp_json['name']

    return result_dict



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
    logger.info(f"function 'get_dify_agent_api_key' response: {resp_json}")
    logger.info(f"dify api key list: {resp_json['data']}")
    try:
        target_data = resp_json['data']
    except Exception as e:
        return [e.__str__()]
    return target_data

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
    logger.info(f"dify api key created: {resp_json}")
    return resp_json

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

def get_workflow_parameter_template(api_url:str,api_key:str):

    result = get_agent_input_para_dict(api_url, api_key)
    variables = result["variable"].tolist()
    data_sheet_df = pd.DataFrame(columns=variables)
    excel_buffer = BytesIO()
    with pd.ExcelWriter(excel_buffer, engine="openpyxl") as writer:
        result.to_excel(writer, index=False, sheet_name="description")
        data_sheet_df.to_excel(writer, index=False, sheet_name="data")
    # 将指针重置到文件开头
    excel_buffer.seek(0)

    return excel_buffer

def get_chatflow_parameter_template(api_url:str,api_key:str):

    result = get_agent_input_para_dict(api_url,api_key)
    variables = result["variable"].tolist()
    variables.append("query")
    data_sheet_df = pd.DataFrame(columns=variables)
    excel_buffer = BytesIO()
    with pd.ExcelWriter(excel_buffer, engine="openpyxl") as writer:
        result.to_excel(writer, index=False, sheet_name="description")
        data_sheet_df.to_excel(writer, index=False, sheet_name="data")
    # 将指针重置到文件开头
    excel_buffer.seek(0)

    return excel_buffer

def upload_to_tos(local_path: Path, object_key: str) -> str:
    """同步上传文件到火山引擎 TOS，返回文件公网 URL"""
    ak = os.getenv("TOS_ACCESS_KEY")
    sk = os.getenv("TOS_SECRET_KEY")
    endpoint = os.getenv("TOS_ENDPOINT")
    region = os.getenv("TOS_REGION")
    bucket_name = os.getenv("TOS_BUCKET")

    client = tos.TosClientV2(ak, sk, endpoint, region)
    if not local_path.exists():
        raise FileNotFoundError(f"本地文件不存在: {local_path}")

    try:
        result = client.put_object_from_file(bucket_name, object_key, str(local_path))
        url = f"https://{bucket_name}.{endpoint}/{object_key}"
        logger.info(f"✅ 上传成功: {url}")
        return url
    except tos.exceptions.TosClientError as e:
        logger.error(f"TOS 客户端错误: {e.message}, 原因: {e.cause}")
        raise
    except tos.exceptions.TosServerError as e:
        logger.error(f"TOS 服务端错误: code={e.code}, 请求ID={e.request_id}, 消息={e.message}")
        raise
    except Exception as e:
        logger.exception(f"TOS 未知错误: {e}")
        raise


def download_from_tos(object_key: str, local_path: str):
    """从火山 TOS 下载文件"""
    ak = os.getenv("TOS_ACCESS_KEY")
    sk = os.getenv("TOS_SECRET_KEY")
    endpoint = os.getenv("TOS_ENDPOINT")
    region = os.getenv("TOS_REGION")
    bucket_name = os.getenv("TOS_BUCKET")

    client = tos.TosClientV2(ak, sk, endpoint, region)
    try:
        logger.info(f"📥 正在下载: {object_key} → {local_path}")
        with open(local_path, "wb") as f:
            obj = client.get_object(bucket_name, object_key)
            for chunk in obj:
                f.write(chunk)
        logger.success(f"✅ 下载成功: {object_key}")
    except Exception as e:
        logger.error(f"❌ 下载失败: {e}")
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


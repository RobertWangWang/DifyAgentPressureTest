import requests
import pandas as pd
import time
import json
from transformers import AutoTokenizer

from app.utils.logger import logger
from app.utils.provider_models_util import (
    send_message_volcengine_ark,
    send_message_openai_compatible,
    send_message_aliyun_dashscope
)
from app.models.test_record import AgentType

tokenizer = AutoTokenizer.from_pretrained("/home/robertwang/PycharmProjects/DifyAgentPressureTest/app/utils/tokenizer", local_files_only=True)

def single_test_chatflow_non_stream_pressure(
        input_dify_url:str,
        input_dify_api_key:str,
        input_query:str,
        input_dify_username: str,
        llm,
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
            llm_response = llm_func(llm_record.get('config'),answer, ref_answer)
            llm_scorrer = llm_response['json']["choices"][0]["message"]["content"]
            try:
                sccore = json.loads(llm_scorrer.replace("```json","").replace("```","").replace("json",""))
            except Exception as e:
                logger.error(e)
                logger.error(f"llm_scorrer: {llm_scorrer}")

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
        logger.error(e)
        logger.error(f"response: {response.text}")
        result_dict = {}
        result_dict["time_consumption"] = end - start
        result_dict["token_num"] = 0
        result_dict["TPS"] = 0
        result_dict["score"] = 0
        return result_dict

def single_test_workflow_non_stream_pressure(
        input_dify_url:str,
        input_dify_api_key:str,
        input_dify_username: str,
        llm,
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
            llm_response = llm_func(llm_record.get('config'),answer, ref_answer)
            llm_scorrer = llm_response['json']["choices"][0]["message"]["content"]
            try:
                sccore = json.loads(llm_scorrer.replace("```json","").replace("```","").replace("json",""))
            except Exception as e:
                logger.error(e)
                logger.error(f"llm_scorrer: {llm_scorrer}")

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
        answer = str(json_text["data"]["outputs"])
        logger.error(e)
        logger.error(f"response: {answer}")
        result_dict = {}
        result_dict["time_consumption"] = end - start
        result_dict["token_num"] = 0
        result_dict["TPS"] = 0
        result_dict["score"] = 0
        return result_dict

# 验证函数，验证输入参数是否符合dify agent的输入参数要求
def validate_entry(entry: dict, para_df: pd.DataFrame):
    errors = []
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
    logger.info(f"dify api key list: {resp_json['data']}")
    return resp_json['data']

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
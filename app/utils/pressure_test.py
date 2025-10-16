import requests
import pandas as pd
import tiktoken
import time
import json
encoding = tiktoken.get_encoding("cl100k_base")

def single_test_chatflow_non_stream_pressure(
        input_dify_url:str,
        input_dify_api_key:str,
        input_query:str,
        input_dify_username: str,
        input_data_dict:dict = None,)-> dict:

    """
    :param input_dify_url: dify agent 的 url
    :param input_dify_api_key:  dify agent 的 apikey
    :param input_query: 输入的测试query （独立参数，对应dify sys.query）
    :param input_dify_username： dify用户名
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

    json_text = json.loads(response.text)
    tokens = encoding.encode(json_text["answer"])
    result_dict = {}
    result_dict["time_consumption"] = end - start
    result_dict["token_num"] = len(tokens)
    result_dict["TPS"] = result_dict["token_num"] / result_dict["time_consumption"]

    return result_dict

# 验证函数
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
        errors.append(f"Unexpected fields in entry: {extra_fields}")

    return errors
from pathlib import Path
import pandas as pd
import requests
import numpy as np

from app.utils.pressure_test import single_test_chatflow_non_stream_pressure,validate_entry
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


def test_chatflow_non_stream_pressure_wrapper(testrecord:TestRecord):

    input_dify_url = testrecord.dify_api_url
    input_dify_api_key = testrecord.dify_api_key
    input_query = testrecord.chatflow_query
    input_dify_username = testrecord.dify_username
    input_dify_test_file = Path("uploads/" + testrecord.filename).resolve()

    if input_dify_test_file.__str__().endswith(".csv"):
        df = pd.read_csv(input_dify_test_file)
    elif input_dify_test_file.__str__().endswith(".xlsx"):
        df = pd.read_excel(input_dify_test_file,engine="openpyxl")

    ### 1.获取智能体可输入的参数字典
    para_df = get_agent_input_para_dict(input_dify_url,input_dify_api_key)

    df = align_dify_input_types(df, para_df)

    ### 2.验证用户上传的文件中的参数是否符合智能体的参数要求
    for index,row in df.iterrows():

        row_dict = row.to_dict()
        error = validate_entry(row_dict, para_df)
        if error:
            return {"error": error}

    ### 3.单条测试
    for index,row in df.iterrows():
        row_dict = row.to_dict()
        single_test_chatflow_non_stream_pressure(input_dify_url, input_dify_api_key, input_query, input_dify_username, row_dict)
        break

    ## input_data_dict
    return input_dify_test_file
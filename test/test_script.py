import requests

def upload_test_chatflow_record(
    url: str,
    filepath: str,
    status: str,
    duration: int | None = None,
    result: str | None = None,
    concurrency: int | None = None,
    dify_api_url: str = "",
    dify_api_key: str = "",
    dify_username: str = "",
    chatflow_query: str = ""
):
    # 打开文件，注意以二进制模式打开
    with open(filepath, "rb") as f:
        files = {
            "file": (filepath, f, "text/csv")  # 第一个元素是 filename，第二是 file 对象，第三是 MIME type
        }
        # 构造表单字段
        data = {
            "status": status,
            "dify_api_url": dify_api_url,
            "dify_api_key": dify_api_key,
            "dify_username": dify_username,
            "chatflow_query": chatflow_query
        }
        if duration is not None:
            data["duration"] = str(duration)
        if result is not None:
            data["result"] = result
        if concurrency is not None:
            data["concurrency"] = str(concurrency)

        # 发送请求
        resp = requests.post(url, data=data, files=files)
        return resp

if __name__ == "__main__":
    url = "http://127.0.0.1:8000/test_chatflow_records/"  # 替换成你的接口地址
    filepath = "/home/robertwang/PycharmProjects/DifyAgentPressureTest/test/example.csv"
    response = upload_test_chatflow_record(
        url=url,
        filepath=filepath,
        status="init",
        duration=10,
        result="pending",
        concurrency=5,
        dify_api_url="https://api.example.com/test",
        dify_api_key="abcd1234",
        dify_username="robertwang",
        chatflow_query="What is the capital of France?"
    )
    print("Status code:", response.status_code)
    print("Response body:", response.text)

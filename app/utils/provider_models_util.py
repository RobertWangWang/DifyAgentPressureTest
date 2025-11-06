import aiohttp
import requests
from urllib.parse import urlparse
import asyncio
from concurrent.futures import ThreadPoolExecutor
executor = ThreadPoolExecutor(max_workers=128)
asyncio.get_event_loop().set_default_executor(executor)

from app.utils.logger import logger
JUDGE_PROMPT = """
你是一名语义相似度评估员。

现在给出两段文本 A 和 B，请根据它们在语义上的接近程度打一个 0–100 的分数。
评分标准如下：
- 0 分：语义完全不同，没有任何关联；
- 100 分：语义完全相同；
- 50 分：部分相似，有部分语义重合但主要意思不同。

请只输出一个整数分数（不需要解释）。

文本A：{gen_text}
文本B：{ref_text}

相似度评分（0–100）：

以json的格式返回你的结果，json的格式如下：
{"score":《你给出的分数》}

注意，返回格式只能是json，不要输出你的思考内容

"""

# 尝试导入 Volcengine Ark 的 SDK（如果你安装了的话）
try:
    from volcenginesdkarkruntime import Ark
    _has_volc_sdk = True
    logger.info("Volcengine SDK 已导入，后续调用会优先使用 SDK")
except ImportError:
    _has_volc_sdk = False
    logger.warning("Volcengine SDK 导入失败，将使用 HTTP 请求方式代替")

def normalize_endpoint(raw: str, default_base: str = None) -> str:
    """
    如果 raw 已经包含 scheme（http/https），直接返回；
    否则，如果给定了 default_base，则拼接；否则默认加 https://
    """
    if not raw:
        return raw
    raw = raw.strip()
    parsed = urlparse(raw)
    if parsed.scheme in ("http", "https"):
        return raw
    if default_base:
        return default_base.rstrip("/") + "/" + raw.lstrip("/")
    return "https://" + raw

def call_aliyun_dashscope(config: dict) -> dict:
    try:
        endpoint = normalize_endpoint(config.get("endpointId"))
        api_key = config.get("apiKey")
        model = config.get("apiEndpointModelName")
        if not (endpoint and api_key and model):
            return {"error": "缺少 endpoint / apiKey / model 配置"}
        payload = {
            "model": model,
            "messages": [
                {"role": "system", "content": "You are a helpful assistant."},
                {"role": "user", "content": "Hello"}
            ]
        }
        resp = requests.post(
            endpoint,
            headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
            json=payload
        )
        try:
            js = resp.json()
        except:
            js = None
        return {"status": resp.status_code, "text": resp.text[:300], "json": js}
    except Exception as e:
        return {"error": str(e)}


async def send_message_aliyun_dashscope(
    config: dict,
    gen_text: str,
    ref_text: str,
    aiohttp_session: aiohttp.ClientSession,
    judge_prompt: str = JUDGE_PROMPT,
) -> dict:
    """
    异步版 - 发送消息到阿里云 DashScope 模型接口
    """

    try:
        endpoint = normalize_endpoint(config.get("endpointId"))
        api_key = config.get("apiKey")
        model = config.get("apiEndpointModelName")

        if not (endpoint and api_key and model):
            return {"error": "缺少 endpoint / apiKey / model 配置"}

        payload = {
            "model": model,
            "messages": [
                {"role": "system", "content": judge_prompt},
                {"role": "user", "content": f"生成的文本： {gen_text}"},
                {"role": "user", "content": f"参考文本： {ref_text}"}
            ]
        }

        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json"
        }

        # ✅ 使用异步 session 发送请求
        async with aiohttp_session.post(endpoint, headers=headers, json=payload) as resp:
            status = resp.status
            text = await resp.text()

            try:
                js = await resp.json(content_type=None)
            except Exception:
                js = None

            logger.debug(f"[DashScope] {status=}, {text[:200]}")

            return {"status": status, "text": text, "json": js}

    except Exception as e:
        logger.exception("❌ DashScope 请求异常")
        return {"error": str(e)}

def call_openai_compatible(config: dict) -> dict:
    try:
        endpoint = normalize_endpoint(config.get("endpointId"))
        api_key = config.get("apiKey")
        model = config.get("apiEndpointModelName")
        if not (endpoint and api_key and model):
            return {"error": "缺少 endpoint / apiKey / model 配置"}
        payload = {
            "model": model,
            "messages": [
                {"role": "system", "content": "You are an AI assistant."},
                {"role": "user", "content": "Hello"}
            ]
        }
        resp = requests.post(
            endpoint,
            headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
            json=payload
        )
        try:
            js = resp.json()
        except:
            js = None
        return {"status": resp.status_code, "text": resp.text[:300], "json": js}
    except Exception as e:
        return {"error": str(e)}


async def send_message_openai_compatible(
    config: dict,
    gen_text: str,
    ref_text: str,
    aiohttp_session: aiohttp.ClientSession,
    judge_prompt: str = JUDGE_PROMPT,
) -> dict:
    """
    异步版：调用 OpenAI-compatible API（如 OpenAI / vLLM / Qwen / Ollama 等兼容端点）
    """
    try:
        endpoint = normalize_endpoint(config.get("endpointId"))
        api_key = config.get("apiKey")
        model = config.get("apiEndpointModelName")

        if not (endpoint and api_key and model):
            return {"error": "缺少 endpoint / apiKey / model 配置"}

        payload = {
            "model": model,
            "messages": [
                {"role": "system", "content": judge_prompt},
                {"role": "user", "content": f"生成的文本： {gen_text}"},
                {"role": "user", "content": f"参考文本： {ref_text}"},
            ],
        }

        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        }

        # ✅ 使用 aiohttp 异步请求
        async with aiohttp_session.post(endpoint, headers=headers, json=payload) as resp:
            status = resp.status
            text = await resp.text()

            try:
                js = await resp.json(content_type=None)
            except Exception:
                js = None

            logger.debug(f"[OpenAI Compatible] {status=}, text[:150]={text[:150]!r}")

            return {"status": status, "text": text, "json": js}

    except Exception as e:
        logger.exception("❌ OpenAI-compatible 调用异常")
        return {"error": str(e)}


def call_volcengine_ark(config: dict) -> dict:
    """
    调用火山方舟 Ark 的对话 Chat 接口。
    根据官方文档，API 地址固定为 https://ark.cn-beijing.volces.com/api/v3/chat/completions :contentReference[oaicite:0]{index=0}
    我们把 config 里的 model ID（apiEndpointModelName 或 endpointId）作为 “model” 字段传入。
    """
    logger.debug(f"call_volcengine_ark: {config}")
    api_key = config.get("apiKey")
    # 模型 ID 可以存在 apiEndpointModelName 或 endpointId 字段里
    model = config.get("apiEndpointModelName") or config.get("endpointId")
    if not (api_key and model):
        return {"error": "config 中缺少 apiKey 或 model"}

    # 如果 SDK 可用，尝试用 SDK
    if _has_volc_sdk:
        try:
            client = Ark(api_key=api_key)
            resp = client.chat.completions.create(
                model=model,
                messages=[{"role": "user", "content": "Hello"}]
            )
            return {"status": 200, "text": str(resp), "sdk_resp": resp}
        except Exception as e:
            logger.warning(f"使用 SDK 调用 Ark 失败，回退 HTTP 方法: {e}")

    # HTTP fallback：使用官方接口地址（不再依赖 config 的 endpointId 作为 URL）
    try:
        endpoint = "https://ark.cn-beijing.volces.com/api/v3/chat/completions"
        payload = {
            "model": model,
            "messages": [
                {"role": "system", "content": "You are a Doubao assistant."},
                {"role": "user", "content": "Hello"}
            ]
        }
        resp = requests.post(
            endpoint,
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json"
            },
            json=payload
        )
        try:
            js = resp.json()
        except:
            js = None
        return {"status": resp.status_code, "text": resp.text[:300], "json": js}
    except Exception as e:
        return {"error": str(e)}

async def send_message_volcengine_ark(
    config: dict,
    gen_text: str,
    ref_text: str,
    aiohttp_session: aiohttp.ClientSession,
    judge_prompt: str = JUDGE_PROMPT,
) -> dict:
    """
    异步版：优先使用 SDK 调用 Ark，否则回退 HTTP 调用。
    """
    api_key = config.get("apiKey")
    model = config.get("apiEndpointModelName") or config.get("endpointId")

    if not (api_key and model):
        return {"error": "config 中缺少 apiKey 或 model"}

    # ✅ 优先使用 SDK
    if _has_volc_sdk:
        try:
            client = Ark(api_key=api_key)
            resp = await asyncio.to_thread(
                client.chat.completions.create,
                model=model,
                messages=[
                    {"role": "system", "content": judge_prompt},
                    {"role": "user", "content": f"生成的文本： {gen_text}"},
                    {"role": "user", "content": f"参考文本： {ref_text}"}
                ]
            )
            logger.warning(f"使用 SDK 调用 Ark 成功，返回结果：{resp}")

            # 🧩 SDK 返回 ChatCompletion 对象
            answer = resp.choices[0].message.content
            return {"status": 200, "text": answer, "sdk_resp": resp}
        except Exception as e:
            logger.warning(f"使用 SDK 调用 Ark 失败，回退 HTTP 方法: {e}")

    # ✅ HTTP fallback（异步）
    try:
        endpoint = "https://ark.cn-beijing.volces.com/api/v3/chat/completions"
        payload = {
            "model": model,
            "messages": [
                {"role": "system", "content": judge_prompt},
                {"role": "user", "content": f"生成的文本： {gen_text}"},
                {"role": "user", "content": f"参考文本： {ref_text}"}
            ]
        }

        headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json"
        }

        async with aiohttp_session.post(endpoint, headers=headers, json=payload) as resp:
            status = resp.status
            text = await resp.text()

            try:
                js = await resp.json(content_type=None)
            except Exception:
                js = None

            logger.debug(f"[Ark HTTP] {status=}, {text[:150]=}")

            return {"status": status, "text": text, "json": js}

    except Exception as e:
        logger.exception("❌ Ark HTTP 调用异常")
        return {"error": str(e)}
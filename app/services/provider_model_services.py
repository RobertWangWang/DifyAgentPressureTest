from typing import List

from app.models.provider_model import ProviderModel
from app.utils.logger import logger
from app.utils.provider_models_util import (
    call_volcengine_ark,
    call_aliyun_dashscope,
    call_openai_compatible,
    send_message_aliyun_dashscope,
    send_message_openai_compatible,
    send_message_volcengine_ark
)

def llm_connection_test(candidate_models: List[ProviderModel]):

    for candidate_model in candidate_models:

        candidate_model_name = candidate_model.model_name
        candidate_model_provider_name = candidate_model.provider_name
        candidate_model_config = candidate_model.config
        if "aliyun" in candidate_model_provider_name or "bailian" in candidate_model_provider_name:
            result = call_aliyun_dashscope(candidate_model_config)
            llm_func = send_message_aliyun_dashscope
        elif "openai" in candidate_model_provider_name:
            result = call_openai_compatible(candidate_model_config)
            llm_func = send_message_openai_compatible
        elif ("volcengine" in candidate_model_provider_name or
              "doubao" in candidate_model_provider_name or
              "ark.cn-beijing" in (candidate_model_config.get("endpointId") or "")):
            result = call_volcengine_ark(candidate_model_config)
            llm_func = send_message_volcengine_ark
        else:
            logger.warning(f" 未识别 provider={candidate_model_provider_name}, 跳过模型 {candidate_model_name}")
            continue
        if result.get('status') == 200:
            logger.info(
                f"Provider={candidate_model_provider_name} | Model={candidate_model_name} | status={result.get('status')} | text={result.get('text')}")
            return {"llm_record":candidate_model.to_dict(),"llm_func":llm_func.__name__ }
    return ""